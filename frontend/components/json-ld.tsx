// Renders a JSON-LD block. Structured data must be in the server-rendered HTML
// to be useful — AI crawlers generally don't execute JavaScript — so this stays
// a server component and never runs on the client.
export function JsonLd({ data }: { data: Record<string, unknown> | Record<string, unknown>[] }) {
  return (
    <script
      type="application/ld+json"
      // JSON.stringify output is escaped for the one sequence that can break
      // out of a <script> block.
      dangerouslySetInnerHTML={{
        __html: JSON.stringify(data).replace(/</g, "\\u003c"),
      }}
    />
  );
}
