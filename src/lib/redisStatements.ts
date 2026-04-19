export function splitRedisStatements(input: string) {
  const statements: string[] = [];
  let buffer = "";
  let quote: '"' | "'" | null = null;
  let escaping = false;

  for (const character of input) {
    if (escaping) {
      buffer += character;
      escaping = false;
      continue;
    }

    if (character === "\\") {
      buffer += character;
      escaping = true;
      continue;
    }

    if (quote) {
      buffer += character;
      if (character === quote) {
        quote = null;
      }
      continue;
    }

    if (character === '"' || character === "'") {
      buffer += character;
      quote = character;
      continue;
    }

    if (character === ";" || character === "\n" || character === "\r") {
      const statement = buffer.trim();
      if (statement) {
        statements.push(statement);
      }
      buffer = "";
      continue;
    }

    buffer += character;
  }

  const finalStatement = buffer.trim();
  if (finalStatement) {
    statements.push(finalStatement);
  }

  return statements;
}
