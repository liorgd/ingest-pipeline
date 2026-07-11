"""ingest-pipeline — M8: the answer composer (the LLM seam).

CI tests what we control (retrieval, prompt construction) and stubs what we
don't (the language model's wording). Swapping in a real LLM changes only
compose_answer's final step; the prompt contract stays identical — which is
exactly why the tests assert on the PROMPT, not on the model's prose.
"""

import os

LLM_BACKEND = os.environ.get("LLM_BACKEND", "stub")

PROMPT_TEMPLATE = """Answer the question using ONLY the passages below.
If the passages do not contain the answer, say so.

Question: {question}

Passages:
{passages}

Answer:"""


def build_prompt(question: str, passages: list[str]) -> str:
    numbered = "\n".join(f"[{i + 1}] {p}" for i, p in enumerate(passages))
    return PROMPT_TEMPLATE.format(question=question, passages=numbered)


def compose_answer(question: str, passages: list[str]) -> tuple[str, str]:
    """Returns (answer, prompt). The prompt is returned for testability."""
    prompt = build_prompt(question, passages)
    if LLM_BACKEND == "stub":
        if not passages:
            return "No relevant passages were found for this question.", prompt
        answer = (
            f"Based on {len(passages)} retrieved passage(s), the most relevant "
            f'states: "{passages[0]}"'
        )
        return answer, prompt
    raise NotImplementedError(  # the seam: plug a real model call here
        f"LLM backend '{LLM_BACKEND}' not implemented"
    )
