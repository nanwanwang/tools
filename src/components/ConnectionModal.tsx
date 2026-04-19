import { useEffect, useState, type FormEvent } from "react";
import { X } from "lucide-react";
import { useI18n } from "../i18n";
import type { ConnectionDraft, MiddlewareKind, TdengineProtocol } from "../types";
import { authModes, createEmptyDraft, getDefaultPort, kindLabels } from "../lib/mockData";

interface ConnectionModalProps {
  open: boolean;
  mode: "create" | "edit";
  initialDraft: ConnectionDraft;
  onClose: () => void;
  onSave: (draft: ConnectionDraft) => Promise<void> | void;
}

function fieldLabelKeyForKind(kind: MiddlewareKind) {
  if (kind === "redis") {
    return "connection.dbIndex";
  }

  if (kind === "kafka") {
    return "connection.defaultTopicFilter";
  }

  if (kind === "tdengine") {
    return "connection.defaultDatabase";
  }

  return "connection.database";
}

export function ConnectionModal({ open, mode, initialDraft, onClose, onSave }: ConnectionModalProps) {
  const { t, kindLabel, authModeLabel, environmentLabel } = useI18n();
  const [draft, setDraft] = useState(initialDraft);
  const [errorMessage, setErrorMessage] = useState("");
  const [isSaving, setIsSaving] = useState(false);
  const [portManuallyEdited, setPortManuallyEdited] = useState(false);

  useEffect(() => {
    setDraft(
      initialDraft.kind === "tdengine" && !initialDraft.protocol
        ? {
            ...initialDraft,
            protocol: "ws",
            port: initialDraft.port || getDefaultPort("tdengine", "ws"),
          }
        : initialDraft,
    );
    setErrorMessage("");
    setPortManuallyEdited(false);
  }, [initialDraft, open]);

  if (!open) {
    return null;
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!draft.name.trim() || !draft.host.trim() || !draft.port) {
      setErrorMessage(t("connection.requiredFields"));
      return;
    }

    setIsSaving(true);
    setErrorMessage("");

    try {
      await onSave({
        ...draft,
        protocol: draft.kind === "tdengine" ? (draft.protocol || "ws") : "",
        name: draft.name.trim(),
        host: draft.host.trim(),
      });
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : t("connection.saveFailed"));
    } finally {
      setIsSaving(false);
    }
  }

  function update<K extends keyof ConnectionDraft>(key: K, value: ConnectionDraft[K]) {
    setDraft((current) => ({
      ...current,
      [key]: value,
    }));
  }

  function handleKindChange(nextKind: MiddlewareKind) {
    const nextDraft = createEmptyDraft(nextKind);
    const nextProtocol = nextKind === "tdengine" ? draft.protocol || nextDraft.protocol || "ws" : "";
    setDraft((current) => ({
      ...current,
      kind: nextKind,
      protocol: nextProtocol,
      port: getDefaultPort(nextKind, nextProtocol),
      authMode: authModes[nextKind][0].value,
      databaseName: current.kind === nextKind ? current.databaseName : nextDraft.databaseName,
      username: current.kind === nextKind ? current.username : nextDraft.username,
      schemaRegistryUrl: nextKind === "kafka" ? current.schemaRegistryUrl : "",
      groupId: nextKind === "kafka" ? current.groupId : "",
      clientId: nextKind === "kafka" ? current.clientId : "",
    }));
    setPortManuallyEdited(false);
  }

  function handlePortChange(value: string) {
    setPortManuallyEdited(true);
    update("port", Number(value));
  }

  function handleProtocolChange(nextProtocol: TdengineProtocol) {
    setDraft((current) => {
      const currentProtocol = current.protocol || "ws";
      const shouldUpdatePort =
        !portManuallyEdited || current.port === getDefaultPort("tdengine", currentProtocol);

      return {
        ...current,
        protocol: nextProtocol,
        port: shouldUpdatePort ? getDefaultPort("tdengine", nextProtocol) : current.port,
      };
    });

    if (!portManuallyEdited || draft.port === getDefaultPort("tdengine", draft.protocol || "ws")) {
      setPortManuallyEdited(false);
    }
  }

  return (
    <div className="modal-overlay" role="presentation" onClick={onClose}>
      <div className="modal-card" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
        <div className="modal-header">
          <div>
            <p className="eyebrow">{mode === "create" ? t("connection.newConnection") : t("connection.editConnection")}</p>
            <h2>{mode === "create" ? t("connection.addTarget") : t("connection.refineProfile")}</h2>
          </div>
          <button className="icon-button" type="button" onClick={onClose} aria-label={t("connection.closeDialog")}>
            <X size={16} />
          </button>
        </div>

        <form className="modal-body" onSubmit={handleSubmit}>
          <div className="kind-selector">
            {(Object.keys(kindLabels) as MiddlewareKind[]).map((kind) => (
              <button
                key={kind}
                className={kind === draft.kind ? "kind-pill active" : "kind-pill"}
                type="button"
                onClick={() => handleKindChange(kind)}
              >
                {kindLabel(kind)}
              </button>
            ))}
          </div>

          <div className="form-grid">
            <label>
              <span>{t("connection.name")}</span>
              <input value={draft.name} onChange={(event) => update("name", event.target.value)} placeholder={t("connection.namePlaceholder")} />
            </label>
            <label>
              <span>{t("connection.host")}</span>
              <input value={draft.host} onChange={(event) => update("host", event.target.value)} placeholder={t("connection.hostPlaceholder")} />
            </label>
            <label>
              <span>{t("connection.port")}</span>
              <input
                type="number"
                value={draft.port}
                onChange={(event) => handlePortChange(event.target.value)}
                min={1}
                max={65535}
              />
            </label>
            {draft.kind === "tdengine" ? (
              <label>
                <span>{t("connection.protocol")}</span>
                <select value={draft.protocol || "ws"} onChange={(event) => handleProtocolChange(event.target.value as TdengineProtocol)}>
                  <option value="ws">{t("connection.protocolWs")}</option>
                  <option value="native">{t("connection.protocolNative")}</option>
                </select>
              </label>
            ) : null}
            <label>
              <span>{t(fieldLabelKeyForKind(draft.kind))}</span>
              <input
                value={draft.databaseName}
                onChange={(event) => update("databaseName", event.target.value)}
                placeholder={
                  draft.kind === "redis"
                    ? t("connection.databasePlaceholderRedis")
                    : draft.kind === "tdengine"
                      ? t("connection.databasePlaceholderTdengine")
                      : t("connection.databasePlaceholderDefault")
                }
              />
            </label>
            <label>
              <span>{t("connection.username")}</span>
              <input
                value={draft.username}
                onChange={(event) => update("username", event.target.value)}
                placeholder={draft.kind === "tdengine" ? t("connection.usernamePlaceholderTdengine") : t("connection.usernamePlaceholderDefault")}
              />
            </label>
            {draft.kind === "tdengine" ? (
              <label className="full-width">
                <span>{t("connection.protocolNotes")}</span>
                <div className="connection-hint-card">
                  {draft.protocol === "native"
                    ? t("connection.protocolNativeHint")
                    : t("connection.protocolWsHint")}
                </div>
              </label>
            ) : null}
            <label>
              <span>{t("connection.password")}</span>
              <input
                type="password"
                value={draft.password}
                onChange={(event) => update("password", event.target.value)}
                placeholder={mode === "edit" ? t("connection.passwordPlaceholderEdit") : t("connection.passwordPlaceholderCreate")}
              />
            </label>
            <label>
              <span>{t("connection.authMode")}</span>
              <select value={draft.authMode} onChange={(event) => update("authMode", event.target.value)}>
                {authModes[draft.kind].map((option) => (
                  <option key={option.value} value={option.value}>
                    {authModeLabel(draft.kind, option.value)}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>{t("connection.environment")}</span>
              <select value={draft.environment} onChange={(event) => update("environment", event.target.value as ConnectionDraft["environment"])}>
                <option value="local">{environmentLabel("local")}</option>
                <option value="dev">{environmentLabel("dev")}</option>
                <option value="staging">{environmentLabel("staging")}</option>
                <option value="production">{environmentLabel("production")}</option>
              </select>
            </label>

            {draft.kind === "kafka" ? (
              <>
                <label>
                  <span>{t("connection.schemaRegistry")}</span>
                  <input
                    value={draft.schemaRegistryUrl}
                    onChange={(event) => update("schemaRegistryUrl", event.target.value)}
                    placeholder={t("connection.schemaRegistryPlaceholder")}
                  />
                </label>
                <label>
                  <span>{t("connection.clientId")}</span>
                  <input value={draft.clientId} onChange={(event) => update("clientId", event.target.value)} placeholder={t("connection.clientIdPlaceholder")} />
                </label>
                <label>
                  <span>{t("connection.groupId")}</span>
                  <input value={draft.groupId} onChange={(event) => update("groupId", event.target.value)} placeholder={t("connection.groupIdPlaceholder")} />
                </label>
              </>
            ) : null}

            <label className="full-width">
              <span>{t("connection.tags")}</span>
              <input value={draft.tagsInput} onChange={(event) => update("tagsInput", event.target.value)} placeholder={t("connection.tagsPlaceholder")} />
            </label>

            <label className="toggle-field">
              <input type="checkbox" checked={draft.readonly} onChange={(event) => update("readonly", event.target.checked)} />
              <span>{t("connection.readOnlyMode")}</span>
            </label>
            <label className="toggle-field">
              <input type="checkbox" checked={draft.useTls} onChange={(event) => update("useTls", event.target.checked)} />
              <span>{t("connection.tlsEnabled")}</span>
            </label>
            <label className="toggle-field">
              <input type="checkbox" checked={draft.tlsVerify} onChange={(event) => update("tlsVerify", event.target.checked)} />
              <span>{t("connection.verifyHostname")}</span>
            </label>
            <label className="toggle-field">
              <input type="checkbox" checked={draft.sshEnabled} onChange={(event) => update("sshEnabled", event.target.checked)} />
              <span>{t("connection.sshTunnel")}</span>
            </label>

            {draft.sshEnabled ? (
              <>
                <label>
                  <span>{t("connection.sshHost")}</span>
                  <input value={draft.sshHost} onChange={(event) => update("sshHost", event.target.value)} placeholder={t("connection.sshHostPlaceholder")} />
                </label>
                <label>
                  <span>{t("connection.sshPort")}</span>
                  <input
                    type="number"
                    value={draft.sshPort}
                    onChange={(event) => update("sshPort", Number(event.target.value))}
                    min={1}
                    max={65535}
                  />
                </label>
                <label>
                  <span>{t("connection.sshUser")}</span>
                  <input value={draft.sshUsername} onChange={(event) => update("sshUsername", event.target.value)} placeholder={t("connection.sshUserPlaceholder")} />
                </label>
              </>
            ) : null}

            <label className="full-width">
              <span>{t("connection.notes")}</span>
              <textarea
                rows={4}
                value={draft.notes}
                onChange={(event) => update("notes", event.target.value)}
                placeholder={t("connection.notesPlaceholder")}
              />
            </label>
          </div>

          {errorMessage ? <p className="form-error">{errorMessage}</p> : null}

          <div className="modal-footer">
            <button className="ghost-button" type="button" onClick={onClose}>
              {t("connection.cancel")}
            </button>
            <button className="primary-button" type="submit" disabled={isSaving}>
              {isSaving ? t("connection.saving") : mode === "create" ? t("connection.saveConnection") : t("connection.updateConnection")}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
