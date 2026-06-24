from __future__ import annotations

import time
from typing import Any, Dict, Optional, Union

import requests


class VeniceMusicError(RuntimeError):
    def __init__(self, message: str, status_code: Optional[int] = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class VeniceMusicTimeout(VeniceMusicError):
    pass


class VeniceMusicClient:
    """Small sync client for Venice public prompt-based audio endpoints.

    Venice's public music endpoint accepts a text brief, duration, lyrics controls,
    and returns a queue id. It does not receive the user's voice as reference audio;
    callers should analyze the voice locally and send a production prompt.
    """

    def __init__(self, api_key: str, base_url: str, timeout: int = 60):
        self.api_key = (api_key or '').strip()
        self.base_url = (base_url or 'https://api.venice.ai/api/v1').rstrip('/')
        self.timeout = timeout
        if not self.api_key:
            raise VeniceMusicError('VENICE_API_KEY is not configured', status_code=401)
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Accept': 'application/json, audio/*, application/octet-stream',
            'Content-Type': 'application/json',
        })

    def _post(self, path: str, payload: Dict[str, Any]) -> requests.Response:
        resp = self.session.post(f'{self.base_url}{path}', json=payload, timeout=self.timeout)
        if resp.status_code in {401, 402, 429, 500, 503}:
            body: Any
            try:
                body = resp.json()
            except Exception:
                body = resp.text[:500]
            raise VeniceMusicError(f'Venice API error {resp.status_code}', resp.status_code, body)
        if resp.status_code >= 400:
            raise VeniceMusicError(f'Venice API error {resp.status_code}', resp.status_code, resp.text[:500])
        return resp

    def quote(self, model: str, duration_seconds: int, character_count: Optional[int] = None) -> Optional[float]:
        payload = {'model': model, 'duration_seconds': duration_seconds}
        if character_count is not None:
            payload['character_count'] = int(character_count)
        resp = self._post('/audio/quote', payload)
        try:
            data = resp.json()
        except Exception:
            return None
        for key in ('quote_usd', 'usd', 'cost_usd', 'total_usd', 'amount'):
            if key in data:
                try:
                    return float(data[key])
                except Exception:
                    pass
        return None

    def queue_music(self, model: str, prompt: str, duration_seconds: int, force_instrumental: bool = True,
                    lyrics_prompt: str = '', lyrics_optimizer: bool = False, language_code: Optional[str] = None) -> str:
        payload: Dict[str, Any] = {
            'model': model,
            'prompt': prompt,
            'lyrics_prompt': lyrics_prompt,
            'duration_seconds': int(duration_seconds),
            'force_instrumental': bool(force_instrumental),
            'lyrics_optimizer': bool(lyrics_optimizer),
        }
        if language_code:
            payload['language_code'] = language_code
        resp = self._post('/audio/queue', payload)
        data = resp.json()
        queue_id = data.get('queue_id') or data.get('id') or data.get('request_id')
        if not queue_id:
            raise VeniceMusicError('Venice queue response did not include queue_id', resp.status_code, data)
        return str(queue_id)

    def retrieve_music(self, model: str, queue_id: str, delete_media_on_completion: bool = False) -> Union[bytes, Dict[str, Any]]:
        resp = self._post('/audio/retrieve', {
            'model': model,
            'queue_id': queue_id,
            'delete_media_on_completion': bool(delete_media_on_completion),
        })
        ctype = (resp.headers.get('content-type') or '').lower()
        if 'application/json' in ctype or ctype.startswith('text/json'):
            return resp.json()
        if ctype.startswith('audio/') or 'octet-stream' in ctype or resp.content[:4] in {b'ID3\x03', b'RIFF'}:
            return resp.content
        try:
            return resp.json()
        except Exception:
            return resp.content

    def complete(self, model: str, queue_id: str) -> bool:
        resp = self._post('/audio/complete', {'model': model, 'queue_id': queue_id})
        if not resp.content:
            return True
        try:
            data = resp.json()
            return bool(data.get('ok', True))
        except Exception:
            return resp.ok

    def generate_music(self, prompt: str, model: str, duration_seconds: int, force_instrumental: bool = True,
                       max_poll_seconds: int = 360, poll_interval_seconds: int = 5) -> bytes:
        queue_id = self.queue_music(model, prompt, duration_seconds, force_instrumental=force_instrumental)
        deadline = time.monotonic() + max_poll_seconds
        while time.monotonic() < deadline:
            result = self.retrieve_music(model, queue_id, delete_media_on_completion=False)
            if isinstance(result, (bytes, bytearray)):
                return bytes(result)
            status = str(result.get('status') or result.get('state') or '').upper()
            if status in {'FAILED', 'ERROR', 'CANCELLED', 'CANCELED'}:
                raise VeniceMusicError('Venice generation failed', payload=result)
            time.sleep(max(1, poll_interval_seconds))
        raise VeniceMusicTimeout('Venice generation timed out')
