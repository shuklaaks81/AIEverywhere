from __future__ import annotations

from threading import Lock
from uuid import uuid4

from langchain.memory import ConversationBufferMemory

_memory_store: dict[str, ConversationBufferMemory] = {}
_memory_lock = Lock()


def get_or_create_conversation_id(conversation_id: str | None) -> str:
    return conversation_id or str(uuid4())


def get_memory(conversation_id: str) -> ConversationBufferMemory:
    with _memory_lock:
        if conversation_id not in _memory_store:
            _memory_store[conversation_id] = ConversationBufferMemory(
                return_messages=True,
                memory_key="chat_history",
                input_key="input",
                output_key="output",
            )
        return _memory_store[conversation_id]


def load_chat_history(conversation_id: str) -> str:
    memory = get_memory(conversation_id)
    messages = memory.load_memory_variables({}).get("chat_history", [])
    if not messages:
        return ""

    formatted_messages: list[str] = []
    for message in messages[-6:]:
        role = getattr(message, "type", "message")
        formatted_messages.append(f"{role}: {message.content}")
    return "\n".join(formatted_messages)


def save_turn(conversation_id: str, user_query: str, answer: str) -> None:
    memory = get_memory(conversation_id)
    memory.save_context({"input": user_query}, {"output": answer})
