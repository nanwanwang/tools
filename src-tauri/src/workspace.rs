use crate::models::{
    ConnectionRecord, ResourceNode, WorkspaceAction, WorkspaceMetric, WorkspacePanel,
    WorkspaceSnapshot,
};

fn resource(
    id: &str,
    label: &str,
    kind: &str,
    meta: Option<&str>,
    children: Option<Vec<ResourceNode>>,
) -> ResourceNode {
    ResourceNode {
        id: id.to_string(),
        label: label.to_string(),
        kind: kind.to_string(),
        meta: meta.map(|value| value.to_string()),
        children,
        expandable: None,
    }
}

fn metric(label: &str, value: &str, detail: &str, tone: &str) -> WorkspaceMetric {
    WorkspaceMetric {
        label: label.to_string(),
        value: value.to_string(),
        detail: detail.to_string(),
        tone: tone.to_string(),
    }
}

fn panel(
    eyebrow: &str,
    title: &str,
    description: &str,
    content: &str,
    language: &str,
) -> WorkspacePanel {
    WorkspacePanel {
        eyebrow: eyebrow.to_string(),
        title: title.to_string(),
        description: description.to_string(),
        content: content.to_string(),
        language: language.to_string(),
    }
}

fn action(title: &str, description: &str, tone: &str) -> WorkspaceAction {
    WorkspaceAction {
        title: title.to_string(),
        description: description.to_string(),
        tone: tone.to_string(),
    }
}

pub fn build_workspace_snapshot(connection: &ConnectionRecord) -> WorkspaceSnapshot {
    let base_capabilities = vec![
        "TLS".to_string(),
        "SSH Tunnel".to_string(),
        "Read-only guard".to_string(),
    ];

    if connection.kind == "redis" {
        return WorkspaceSnapshot {
            connection_id: connection.id.clone(),
            title: "Redis browser".into(),
            subtitle: "Prefix-aware key explorer with TTL controls and stream inspection.".into(),
            capability_tags: vec![
                base_capabilities[0].clone(),
                base_capabilities[1].clone(),
                base_capabilities[2].clone(),
                "SCAN paging".into(),
                "TTL edit".into(),
                "Pub/Sub preview".into(),
            ],
            metrics: vec![
                metric("Live keys", "14.2k", "SCAN-backed browse", "accent"),
                metric("Hot prefixes", "28", "Grouped by namespace", "neutral"),
                metric(
                    "Write safety",
                    if connection.readonly { "Locked" } else { "Guarded" },
                    "Danger actions ask again",
                    if connection.readonly { "success" } else { "danger" },
                ),
            ],
            resources: vec![resource(
                &format!("db:{}", connection.database_name),
                &format!("db{}", connection.database_name),
                "database",
                Some("14.2k keys"),
                Some(vec![
                    resource(
                        "group:sessions",
                        "sessions:*",
                        "prefix",
                        Some("5.8k keys"),
                        Some(vec![
                            resource("key:session:2048", "session:2048", "string", Some("TTL 1h"), None),
                            resource("key:session:9042", "session:9042", "string", Some("TTL 15m"), None),
                        ]),
                    ),
                    resource(
                        "group:cache",
                        "cache:*",
                        "prefix",
                        Some("3.1k keys"),
                        Some(vec![
                            resource("key:cache:feed", "cache:feed", "hash", Some("9 fields"), None),
                            resource("key:cache:flags", "cache:flags", "set", Some("18 members"), None),
                        ]),
                    ),
                    resource("group:stream", "orders.stream", "stream", Some("12 groups"), None),
                ]),
            )],
            panels: vec![
                panel(
                    "Value Preview",
                    "session:2048",
                    "Structured JSON preview with TTL metadata kept nearby.",
                    "{\n  \"userId\": 2048,\n  \"region\": \"ap-southeast-1\",\n  \"flags\": [\"beta\", \"priority\"],\n  \"expiresIn\": 3600\n}",
                    "json",
                ),
                panel(
                    "Operational Notes",
                    "Key hygiene",
                    "Show large keys, expiring sessions and stream backlog in one panel.",
                    "SCAN cursor: 481\nSlowlog threshold: 10000us\nTop prefix: sessions:*",
                    "text",
                ),
            ],
            actions: vec![
                action("Edit value", "Open structured editor and keep the original TTL visible.", "accent"),
                action(
                    "Delete with confirm",
                    "Single and batch delete stay behind an explicit confirmation step.",
                    "danger",
                ),
                action("Inspect streams", "Review consumer groups, pending counts and recent records.", "neutral"),
            ],
            diagnostics: vec![
                "Use SCAN paging instead of KEYS for large instances.".into(),
                "Mask passwords and ACL secrets in logs and exported bundles.".into(),
                "Treat production aliases as guarded targets with red emphasis.".into(),
            ],
        };
    }

    if connection.kind == "kafka" {
        return WorkspaceSnapshot {
            connection_id: connection.id.clone(),
            title: "Kafka workspace".into(),
            subtitle: "Consume, inspect and publish messages without leaving the desktop shell.".into(),
            capability_tags: vec![
                base_capabilities[0].clone(),
                base_capabilities[1].clone(),
                base_capabilities[2].clone(),
                "Offset jumps".into(),
                "JSON formatting".into(),
                "Schema Registry".into(),
            ],
            metrics: vec![
                metric("Topics", "42", "Searchable and partition-aware", "accent"),
                metric("Consumer groups", "17", "Lag sorted", "neutral"),
                metric("Guard rails", "Offset reset off", "Bulk reset disabled by default", "success"),
            ],
            resources: vec![
                resource(
                    "topic:orders",
                    "orders.created",
                    "topic",
                    Some("12 partitions"),
                    Some(vec![
                        resource("partition:0", "partition-0", "partition", Some("Latest offset 482901"), None),
                        resource("partition:1", "partition-1", "partition", Some("Latest offset 481123"), None),
                    ]),
                ),
                resource("topic:billing", "billing.reconciled", "topic", Some("8 partitions"), None),
                resource("group:checkout", "checkout-service", "consumer-group", Some("Lag 124"), None),
                resource("group:analytics", "analytics-worker", "consumer-group", Some("Lag 0"), None),
            ],
            panels: vec![
                panel(
                    "Message Preview",
                    "orders.created",
                    "Readable JSON, offset position and headers in the same tab.",
                    "{\n  \"orderId\": \"ord_482901\",\n  \"customerId\": \"cus_7712\",\n  \"total\": 188.92,\n  \"currency\": \"USD\"\n}",
                    "json",
                ),
                panel(
                    "Producer Draft",
                    "Safe publish",
                    "Keep headers, key and partition choice visible before sending.",
                    "Partition: auto\nHeaders: content-type=application/json\nSchema: avro subject connected",
                    "text",
                ),
            ],
            actions: vec![
                action("Consume by range", "Jump by partition, offset or time window.", "accent"),
                action("Publish sample", "Draft JSON payloads with validation hints before send.", "neutral"),
                action("Review lag", "Surface consumer groups that drift from the head offset.", "danger"),
            ],
            diagnostics: vec![
                "Separate security mode from schema settings to keep connection forms readable.".into(),
                "Show broker/auth/version failures as plain-language diagnostics.".into(),
                "Keep high-volume message panes virtualized to avoid UI stalls.".into(),
            ],
        };
    }

    WorkspaceSnapshot {
        connection_id: connection.id.clone(),
        title: format!(
            "{} explorer",
            if connection.kind == "mysql" {
                "MySQL"
            } else {
                "PostgreSQL"
            }
        ),
        subtitle: "Schema browser, query tabs and export-ready result sets.".into(),
        capability_tags: vec![
            base_capabilities[0].clone(),
            base_capabilities[1].clone(),
            base_capabilities[2].clone(),
            "Result export".into(),
            "Explain plans".into(),
            "Cancelable queries".into(),
        ],
        metrics: vec![
            metric(
                "Schemas",
                if connection.kind == "postgres" { "6" } else { "1" },
                "Tree navigation",
                "accent",
            ),
            metric("Active tabs", "3", "Pinned query history", "neutral"),
            metric(
                "Write mode",
                if connection.readonly { "Read only" } else { "Confirm first" },
                "DDL/DML guarded",
                if connection.readonly { "success" } else { "danger" },
            ),
        ],
        resources: vec![resource(
            if connection.kind == "postgres" { "schema:public" } else { "schema:app" },
            if connection.kind == "postgres" { "public" } else { "app" },
            "schema",
            Some("18 tables"),
            Some(vec![
                resource("table:orders", "orders", "table", Some("4.2M rows"), None),
                resource("table:payments", "payments", "table", Some("2.1M rows"), None),
                resource("table:users", "users", "table", Some("670k rows"), None),
                resource("view:order_health", "order_health", "view", Some("materialized summary"), None),
            ]),
        )],
        panels: vec![
            panel(
                "Query Pad",
                "Recent SQL",
                "Hold multi-tab query work with explain plans and export actions nearby.",
                "select id, status, created_at\nfrom orders\nwhere created_at >= now() - interval '7 days'\norder by created_at desc\nlimit 50;",
                "sql",
            ),
            panel(
                "Execution Plan",
                "Explain snapshot",
                "Highlight scan type, index usage and rows examined without leaving the result tab.",
                "Index Scan using orders_created_at_idx\nRows: 50\nBuffers: shared hit=312",
                "text",
            ),
        ],
        actions: vec![
            action("Open query tab", "Keep SQL editor, result grid and history in a single workspace.", "accent"),
            action("Export results", "Save current selection as CSV or JSON.", "neutral"),
            action("Protect writes", "Ask again before DDL or large DML statements run.", "danger"),
        ],
        diagnostics: vec![
            "Keep long-running queries cancelable from the UI.".into(),
            "Separate schema tree fetches from result pagination to keep scrolling smooth.".into(),
            "Mask passwords, tokens and certificate values in logs.".into(),
        ],
    }
}
