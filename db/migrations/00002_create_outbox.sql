-- +goose Up
-- +goose StatementBegin
CREATE TABLE outbox (
    id            CHAR(36)     NOT NULL PRIMARY KEY,
    event_type    VARCHAR(100) NOT NULL,
    aggregate_id  CHAR(36)     NOT NULL,
    payload       JSON         NOT NULL,
    created_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at  DATETIME(6)  NULL DEFAULT NULL
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE outbox;
-- +goose StatementEnd
