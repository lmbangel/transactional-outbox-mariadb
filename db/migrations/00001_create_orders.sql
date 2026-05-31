-- +goose Up
-- +goose StatementBegin
CREATE TABLE orders (
    id           CHAR(36)     NOT NULL PRIMARY KEY,
    customer_id  CHAR(36)     NOT NULL,
    product_sku  VARCHAR(100) NOT NULL,
    quantity     INT          NOT NULL,
    status       VARCHAR(50)  NOT NULL DEFAULT 'PENDING',
    created_at   DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE orders;
-- +goose StatementEnd
