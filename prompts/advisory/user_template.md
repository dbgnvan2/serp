## Task

Produce the advisory briefing specified in your system instructions.
Read the inputs below in this order:

1. Strategic flags — these are binding constraints, not suggestions.
   They determine which action is first, which keywords are skip,
   and whether defense takes priority over expansion.
2. Client context — background for assessing fit and framing
   recommendations in the client's language.
3. Market intelligence report — the verified analysis that your
   briefing interprets. Do not repeat it; reference findings by
   keyword name.

## Strategic Flags (binding constraints)

These were computed deterministically from the SERP data. They are
not hypotheses. Use them as given.

- If defensive_urgency = "high", Action 1 must defend the
  declining position.
- content_priorities ordering determines your action sequence.
- Keywords with action = "skip" appear only in "What to Stop
  Thinking About."
- total_results values are indexed page counts, not monthly
  search volume.

<strategic_flags>
{strategic_flags_json}
</strategic_flags>

## Client Context (background — not evidence)

Organization: {client_name}
Website: {client_domain}
Type: {org_type}
Location: {location}
Framework: {framework_description}
Content focus: {content_focus}
Constraints: {additional_context}

Use this to assess whether the client can realistically execute
each recommendation and to frame actions in language that connects
to their work. Do not treat client context as SERP evidence.

## Market Intelligence Report (verified analysis)

This is the first-pass report produced from pre-verified SERP data.
Every number in it has been checked against the raw data. Use it
as your analytical foundation but do not repeat its contents —
the reader has already seen it.

<market_report>
{market_report_text}
</market_report>

Produce the advisory briefing now. Follow the four-part structure
(data → why it matters → what to do → consequence of inaction)
for each of exactly 3 actions.
