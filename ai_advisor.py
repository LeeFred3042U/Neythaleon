"""
ai_advisor.py — Provider-agnostic LLM client and AI capability functions
for the Neythaleon ingestion pipeline.

Supported providers (all optional at import time):
  • anthropic      — claude-sonnet-4-5 via the `anthropic` SDK
  • openai         — gpt-4o via the `openai` SDK
  • ollama         — any local model via HTTP to http://localhost:11434/api/chat
  • openai_compat  — any OpenAI-compatible endpoint (LM Studio, Together, Groq…)
                     via OPENAI_BASE_URL + OPENAI_API_KEY

.env variables:
  LLM_PROVIDER=anthropic          # anthropic | openai | ollama | openai_compat
  LLM_MODEL=claude-sonnet-4-5     # model name, provider-specific
  ANTHROPIC_API_KEY=...
  OPENAI_API_KEY=...
  OPENAI_BASE_URL=...             # only for openai_compat
  OLLAMA_MODEL=llama3             # used if LLM_PROVIDER=ollama (fallback to LLM_MODEL)
"""

import json
import logging
import os

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

logger = logging.getLogger(__name__)
console = Console()


def _resolve_provider() -> str:
    provider = os.environ.get("LLM_PROVIDER", "").strip().lower()
    if provider:
        return provider

    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.environ.get("OPENAI_API_KEY"):
        return "openai"
    return "ollama"


def _resolve_model(provider: str) -> str:
    model = os.environ.get("LLM_MODEL", "").strip()
    if model:
        return model

    defaults = {
        "anthropic": "claude-sonnet-4-5",
        "openai": "gpt-4o",
        "ollama": os.environ.get("OLLAMA_MODEL", "llama3"),
        "openai_compat": "gpt-4o",
    }
    return defaults.get(provider, "gpt-4o")


class LLMClient:

    def __init__(self) -> None:
        self.provider = _resolve_provider()
        self.model = _resolve_model(self.provider)
        logger.info("LLMClient initialised: provider=%s model=%s", self.provider, self.model)

    def chat(self, system: str, user: str, max_tokens: int = 2048) -> str | None:
        try:
            with console.status(
                f"[bold cyan]AI ({self.provider}/{self.model}) thinking…[/bold cyan]",
                spinner="dots",
            ):
                return self._dispatch(system, user, max_tokens)
        except Exception as exc:
            logger.exception("LLM request failed (provider=%s)", self.provider)
            console.print(
                Panel(
                    f"[red]AI request failed:[/red] {exc}\n\n"
                    "[dim]The pipeline will continue without AI assistance.[/dim]",
                    title="[yellow]⚠ AI Unavailable[/yellow]",
                    border_style="yellow",
                )
            )
            return None

    def _dispatch(self, system: str, user: str, max_tokens: int) -> str | None:
        handler = {
            "anthropic": self._call_anthropic,
            "openai": self._call_openai,
            "ollama": self._call_ollama,
            "openai_compat": self._call_openai_compat,
        }.get(self.provider)

        if handler is None:
            logger.error("Unknown LLM provider: %s", self.provider)
            console.print(
                f"[red]Unknown LLM_PROVIDER '{self.provider}'. "
                "Valid values: anthropic, openai, ollama, openai_compat[/red]"
            )
            return None

        return handler(system, user, max_tokens)

    def _call_anthropic(self, system: str, user: str, max_tokens: int) -> str | None:
        try:
            import anthropic  # type: ignore  # noqa: PLC0415
        except ImportError:
            console.print(
                "[yellow]anthropic SDK not installed. Run: pip install anthropic>=0.25.0[/yellow]"
            )
            return None

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            console.print("[yellow]ANTHROPIC_API_KEY not set in .env[/yellow]")
            return None

        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return message.content[0].text

    def _call_openai(self, system: str, user: str, max_tokens: int) -> str | None:
        try:
            import openai  # type: ignore  # noqa: PLC0415
        except ImportError:
            console.print(
                "[yellow]openai SDK not installed. Run: pip install openai>=1.0.0[/yellow]"
            )
            return None

        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            console.print("[yellow]OPENAI_API_KEY not set in .env[/yellow]")
            return None

        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        return response.choices[0].message.content

    def _call_openai_compat(self, system: str, user: str, max_tokens: int) -> str | None:
        try:
            import openai  # type: ignore  # noqa: PLC0415
        except ImportError:
            console.print(
                "[yellow]openai SDK not installed. Run: pip install openai>=1.0.0[/yellow]"
            )
            return None

        base_url = os.environ.get("OPENAI_BASE_URL")
        api_key = os.environ.get("OPENAI_API_KEY", "not-needed")

        if not base_url:
            console.print(
                "[yellow]OPENAI_BASE_URL not set. Required for openai_compat provider.[/yellow]"
            )
            return None

        client = openai.OpenAI(api_key=api_key, base_url=base_url)
        response = client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        return response.choices[0].message.content

    def _call_ollama(self, system: str, user: str, max_tokens: int) -> str | None:
        import urllib.request  # noqa: PLC0415

        model = os.environ.get("OLLAMA_MODEL") or self.model
        url = "http://localhost:11434/api/chat"
        payload = json.dumps(
            {
                "model": model,
                "stream": False,
                "options": {"num_predict": max_tokens},
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            }
        ).encode()

        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read())
                return data["message"]["content"]
        except Exception as exc:
            raise RuntimeError(
                f"Ollama request failed. Is Ollama running at {url}? Error: {exc}"
            ) from exc


_OBIS_CONTEXT = (
    "You are an expert in OBIS (Ocean Biodiversity Information System) marine "
    "biodiversity data. The dataset is H3-gridded species occurrence data with "
    "43 million+ rows. Columns include:\n"
    "  • Taxonomy: kingdom, phylum, class, order, family, genus, species (strings)\n"
    "  • Species ID: AphiaID (INT32, WoRMS identifier, always 100% filled)\n"
    "  • Spatial: cell (H3 hex string, 100% filled), geometry (WKT/WKB, 100% filled)\n"
    "  • Temporal: min_year, max_year (INT64, ~93% filled)\n"
    "  • Occurrence stats: records (INT64, count of occurrences, 100% filled)\n"
    "  • Source flags: source_gbif, source_obis (BOOLEAN, 100% filled)\n"
    "  • Misc: category (BYTE_ARRAY string, only ~5.5% filled — very sparse)\n"
)


def _parse_json_response(raw: str | None, context: str = "") -> dict | list | None:
    if raw is None:
        return None

    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        inner = [l for l in lines[1:] if l.strip() != "```"]
        text = "\n".join(inner).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("AI returned non-JSON for %s. Raw: %r", context, raw[:400])
        return None


def ai_column_advisor(manifest: dict, user_goal: str = "") -> list | None:
    if not user_goal:
        user_goal = Prompt.ask(
            "\n[bold cyan]Describe your analysis goal[/bold cyan] "
            "[dim](e.g. 'species richness per taxon', 'temporal trend of records')[/dim]"
        ).strip()

    if not user_goal:
        return None

    col_lines = []
    for name, meta in manifest.items():
        col_lines.append(
            f"  {name}: physical_type={meta.physical_type}, "
            f"logical_type={meta.logical_type}, "
            f"density_pct={meta.density_pct:.1f}%"
        )

    columns_text = "\n".join(col_lines)

    system_prompt = (
        _OBIS_CONTEXT
        + "\n\nYour task: recommend which columns the user should ingest into PostgreSQL "
        "for their stated goal. Prefer high-density columns (density_pct >= 60%) "
        "unless the goal specifically requires sparse ones. Do NOT recommend "
        "bit-array columns (physical_type=FIXED_LEN_BYTE_ARRAY) unless asked.\n\n"
        "Respond with STRICT JSON ONLY — no markdown fences, no preamble:\n"
        '{"recommended": ["col1", "col2", ...], "explanation": "..."}'
    )

    user_prompt = (
        f"User goal: {user_goal}\n\n"
        f"Available columns:\n{columns_text}"
    )

    client = LLMClient()
    raw = client.chat(system=system_prompt, user=user_prompt, max_tokens=1024)
    parsed = _parse_json_response(raw, context="ai_column_advisor")

    if not isinstance(parsed, dict):
        return None

    recommended = parsed.get("recommended")
    explanation = parsed.get("explanation", "")

    if not isinstance(recommended, list):
        return None

    valid_names = set(manifest.keys())
    recommended = [c for c in recommended if c in valid_names]

    if not recommended:
        console.print("[yellow]AI returned no valid column names.[/yellow]")
        return None

    table = Table(
        title="[bold cyan]AI Column Recommendations[/bold cyan]",
        show_header=True,
        header_style="bold magenta",
        border_style="bright_black",
    )
    table.add_column("#", width=3, justify="right")
    table.add_column("Column", min_width=20)
    table.add_column("Type", min_width=12)
    table.add_column("Density", min_width=8, justify="right")

    for i, name in enumerate(recommended, 1):
        meta = manifest[name]
        density = meta.density_pct
        density_str = f"{density:.1f}%"
        colour = "green" if density >= 60 else "yellow"
        table.add_row(
            str(i),
            name,
            meta.physical_type,
            f"[{colour}]{density_str}[/{colour}]",
        )

    console.print()
    console.print(table)

    if explanation:
        console.print(
            Panel(
                f"[dim]{explanation}[/dim]",
                title="[cyan]AI Reasoning[/cyan]",
                border_style="bright_black",
            )
        )

    total_cols = len(manifest)
    if len(recommended) > total_cols * 0.80:
        console.print(
            Panel(
                f"[yellow]⚠ The AI selected [bold]{len(recommended)}/{total_cols}[/bold] "
                "columns — that is more than 80% of all available columns.\n"
                "This is a very broad selection and may increase storage costs significantly.[/yellow]",
                title="[yellow]Broad Selection Warning[/yellow]",
                border_style="yellow",
            )
        )
        confirm = Prompt.ask(
            "Proceed with this broad selection?", choices=["Y", "N", "y", "n"], default="Y"
        ).upper()
        if confirm != "Y":
            return None

    accept = Prompt.ask(
        "\n[bold]Accept AI column recommendations?[/bold] [Y=accept / N=use manual selector]",
        choices=["Y", "N", "y", "n"],
        default="Y",
    ).upper()

    if accept != "Y":
        return None

    return recommended


def ai_graph_advisor(selected_columns: list, manifest: dict) -> list | None:
    col_lines = []
    for name in selected_columns:
        meta = manifest.get(name)
        if meta is None:
            continue
        col_lines.append(
            f"  {name}: physical_type={meta.physical_type}, "
            f"logical_type={meta.logical_type}, "
            f"density_pct={meta.density_pct:.1f}%"
        )

    columns_text = "\n".join(col_lines)

    system_prompt = (
        _OBIS_CONTEXT
        + "\n\nYou know the following column semantics:\n"
        "  • AphiaID = WoRMS species ID (numeric, good for counting distinct species)\n"
        "  • cell    = H3 hexagonal grid cell string (categorical/spatial grouping)\n"
        "  • genus, family, class = taxonomy hierarchy (categorical, bar charts)\n"
        "  • records = occurrence count per cell (numeric, distribution/scatter)\n"
        "  • min_year, max_year = temporal range (line charts over time)\n"
        "  • source_gbif, source_obis = boolean data source flags (pie/bar)\n\n"
        "Recommend 4–8 meaningful visualisations for the user's selected columns.\n"
        "Respond with STRICT JSON ONLY — a JSON array, no markdown, no preamble:\n"
        '[\n'
        '  {"x": "col", "y": "col_or_null", "type": "scatter|bar|line|histogram|pie", '
        '"label": "short title", "reason": "why this is informative"},\n'
        '  ...\n'
        ']'
    )

    user_prompt = (
        f"Selected columns for visualisation:\n{columns_text}\n\n"
        "Suggest the most informative charts."
    )

    client = LLMClient()
    raw = client.chat(system=system_prompt, user=user_prompt, max_tokens=1500)
    parsed = _parse_json_response(raw, context="ai_graph_advisor")

    if not isinstance(parsed, list):
        return None

    valid_set = set(selected_columns)
    valid_types = {"scatter", "bar", "line", "histogram", "pie"}
    validated = []

    for item in parsed:
        if not isinstance(item, dict):
            continue

        x = item.get("x")
        y = item.get("y") or None
        chart_type = item.get("type", "scatter")
        label = item.get("label", chart_type)
        reason = item.get("reason", "")

        if x not in valid_set:
            logger.debug("ai_graph_advisor: dropping spec, x=%r not in selected_columns", x)
            continue
        if y is not None and y not in valid_set:
            logger.debug("ai_graph_advisor: dropping spec, y=%r not in selected_columns", y)
            continue
        if chart_type not in valid_types:
            chart_type = "scatter"

        validated.append({"x": x, "y": y, "type": chart_type, "label": label, "reason": reason})

    if len(validated) < 2:
        logger.info("ai_graph_advisor: fewer than 2 valid graphs — falling back to rule-based")
        return None

    return validated


def ai_query_builder(
    engine,
    selected_columns: list,
    table_name: str,
    question: str = "",
) -> str | None:
    if not question:
        question = Prompt.ask(
            "\n[bold cyan]What would you like to query?[/bold cyan] "
            "[dim](e.g. 'top 10 genera by total records')[/dim]"
        ).strip()

    if not question:
        return None

    col_summary = ", ".join(selected_columns)

    system_prompt = (
        _OBIS_CONTEXT
        + f"\n\nThe PostgreSQL table is named '{table_name}' and contains these columns:\n"
        f"  {col_summary}\n\n"
        "Generate a single SQL SELECT query that answers the user's question.\n"
        "Rules:\n"
        "  1. Output raw SQL ONLY — no markdown fences, no explanation, no preamble.\n"
        "  2. Always use double-quoted column names (e.g. \"records\").\n"
        "  3. Add LIMIT 1000 unless the question explicitly asks for aggregates.\n"
        "  4. Never use DML or DDL (INSERT, UPDATE, DELETE, DROP, CREATE, etc.).\n"
        "  5. Start the query with SELECT."
    )

    user_prompt = f"Question: {question}"

    client = LLMClient()
    raw = client.chat(system=system_prompt, user=user_prompt, max_tokens=512)

    if raw is None:
        return None

    sql = raw.strip()
    if sql.startswith("```"):
        lines = sql.splitlines()
        sql = "\n".join(l for l in lines[1:] if l.strip() != "```").strip()

    if not sql.upper().lstrip().startswith("SELECT"):
        console.print(
            Panel(
                f"[red]AI returned a non-SELECT statement — rejected for safety.[/red]\n\n"
                f"[dim]Raw AI output (first 200 chars):\n{sql[:200]}[/dim]",
                title="[red]⛔ SQL Safety Guardrail[/red]",
                border_style="red",
            )
        )
        logger.warning("ai_query_builder: rejected non-SELECT output: %r", sql[:200])
        return None

    return sql
