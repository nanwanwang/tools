import { createContext, useContext, useMemo, useState, type ReactNode } from "react";
import enUS from "./locales/en-US";
import zhCN from "./locales/zh-CN";

export type AppLanguage = "en-US" | "zh-CN";

type Dictionary = Record<string, unknown>;

const dictionaries: Record<AppLanguage, Dictionary> = {
  "en-US": enUS,
  "zh-CN": zhCN,
};

const languageStorageKey = "middleware-studio.language";

function resolveMessage(dictionary: Dictionary, key: string) {
  return key.split(".").reduce<unknown>((current, segment) => {
    if (current && typeof current === "object" && segment in current) {
      return (current as Record<string, unknown>)[segment];
    }

    return undefined;
  }, dictionary);
}

function interpolate(template: string, values?: Record<string, string | number | null | undefined>) {
  if (!values) {
    return template;
  }

  return template.replace(/\{(\w+)\}/g, (_, token) => String(values[token] ?? ""));
}

export function translateStatic(language: AppLanguage, key: string, values?: Record<string, string | number | null | undefined>) {
  const dictionary = dictionaries[language];
  const resolved = resolveMessage(dictionary, key);

  if (typeof resolved !== "string") {
    return key;
  }

  return interpolate(resolved, values);
}

function readInitialLanguage(): AppLanguage {
  if (typeof window === "undefined") {
    return "zh-CN";
  }

  const stored = window.localStorage.getItem(languageStorageKey);
  if (stored === "zh-CN" || stored === "en-US") {
    return stored;
  }

  return window.navigator.language.toLowerCase().startsWith("zh") ? "zh-CN" : "en-US";
}

interface I18nValue {
  language: AppLanguage;
  setLanguage: (language: AppLanguage) => void;
  t: (key: string, values?: Record<string, string | number | null | undefined>) => string;
  formatDateTime: (value: string | number | Date) => string;
  kindLabel: (kind: string) => string;
  environmentLabel: (environment: string) => string;
  authModeLabel: (kind: string, value: string) => string;
}

const I18nContext = createContext<I18nValue | null>(null);

export function I18nProvider({ children }: { children: ReactNode }) {
  const [language, setLanguage] = useState<AppLanguage>(() => readInitialLanguage());

  const value = useMemo<I18nValue>(() => {
    return {
      language,
      setLanguage: (nextLanguage) => {
        setLanguage(nextLanguage);
        if (typeof window !== "undefined") {
          window.localStorage.setItem(languageStorageKey, nextLanguage);
        }
      },
      t: (key, values) => translateStatic(language, key, values),
      formatDateTime: (value) =>
        new Intl.DateTimeFormat(language, {
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
        }).format(new Date(value)),
      kindLabel: (kind) => translateStatic(language, `kind.${kind}`),
      environmentLabel: (environment) => translateStatic(language, `environment.${environment}`),
      authModeLabel: (kind, value) => translateStatic(language, `auth.${kind}.${value}`),
    };
  }, [language]);

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n() {
  const context = useContext(I18nContext);
  if (!context) {
    throw new Error("useI18n must be used inside I18nProvider.");
  }

  return context;
}
