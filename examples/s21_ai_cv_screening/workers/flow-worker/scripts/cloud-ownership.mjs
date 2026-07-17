export function ownedDurableNamespaces(names, namespaces) {
  const owners = new Map([
    [`${names.flow}\0FlowDurableObject`, true],
    [`${names.flow}\0WorkspaceDurableObject`, true],
    [`${names.provider}\0MockProviderState`, true],
  ]);
  return namespaces.filter((namespace) =>
    owners.has(`${namespace.script ?? namespace.script_name}\0${namespace.class ?? namespace.class_name}`),
  );
}

export function ownedKvNamespace(names, namespaceId, namespaces) {
  const byId = namespaceId === null
    ? undefined
    : namespaces.find((namespace) => namespace.id === namespaceId);
  const byTitle = namespaces.find((namespace) => namespace.title === names.kv);
  if (byId !== undefined && byId.title !== names.kv) {
    throw new Error("recorded KV id belongs to a different namespace title");
  }
  if (byTitle !== undefined && namespaceId !== null && byTitle.id !== namespaceId) {
    throw new Error("recorded KV title belongs to a different namespace id");
  }
  return byId ?? (namespaceId === null ? byTitle : undefined);
}
