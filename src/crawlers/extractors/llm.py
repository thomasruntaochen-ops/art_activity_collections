from src.core.config import settings


def llm_extraction_enabled() -> bool:
    return settings.llm_enabled and bool(settings.llm_api_key)


def extract_with_llm(raw_text: str) -> dict:
    """Placeholder for future LLM extraction integration.

    Keep hardcoded extraction as default for simple use cases.
    """
    raise NotImplementedError("LLM extraction not implemented yet")
