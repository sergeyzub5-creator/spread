from __future__ import annotations

import json
from uuid import uuid4

from PySide6.QtCore import QByteArray, QObject, QUrl, Signal
from PySide6.QtNetwork import QNetworkAccessManager, QNetworkReply, QNetworkRequest


class QtTelegramBotClient(QObject):
    message_finished = Signal(str, bool, str)

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._network = QNetworkAccessManager(self)
        self._pending_replies: dict[QNetworkReply, str] = {}

    def send_message(self, *, bot_token: str, chat_id: str, text: str, request_id: str | None = None) -> str:
        rid = str(request_id or uuid4().hex)
        payload = QByteArray(
            json.dumps(
                {
                    "chat_id": str(chat_id or "").strip(),
                    "text": str(text or ""),
                }
            ).encode("utf-8")
        )
        request = QNetworkRequest(QUrl(f"https://api.telegram.org/bot{str(bot_token or '').strip()}/sendMessage"))
        request.setHeader(QNetworkRequest.KnownHeaders.ContentTypeHeader, "application/json")
        reply = self._network.post(request, payload)
        self._pending_replies[reply] = rid
        reply.finished.connect(lambda reply=reply: self._on_reply_finished(reply))
        return rid

    def _on_reply_finished(self, reply: QNetworkReply) -> None:
        rid = self._pending_replies.pop(reply, "")
        ok = False
        message = ""
        try:
            body = bytes(reply.readAll()).decode("utf-8", errors="replace")
            if reply.error() != QNetworkReply.NetworkError.NoError:
                message = str(reply.errorString() or "").strip()
                if body:
                    try:
                        parsed = json.loads(body)
                        description = str(parsed.get("description", "")).strip()
                        if description:
                            message = description
                    except Exception:
                        pass
            else:
                parsed = json.loads(body) if body else {}
                ok = bool(parsed.get("ok"))
                if not ok:
                    message = str(parsed.get("description", "")).strip()
        except Exception as exc:
            message = str(exc).strip()
        finally:
            reply.deleteLater()
        self.message_finished.emit(rid, ok, message)
