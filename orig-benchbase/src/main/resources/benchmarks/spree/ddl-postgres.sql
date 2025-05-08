DROP TABLE IF EXISTS spree_users;
DROP TABLE IF EXISTS spree_orders;
DROP TABLE IF EXISTS spree_prices;
DROP TABLE IF EXISTS spree_line_items;
DROP TABLE IF EXISTS spree_tax_categories;
DROP TABLE IF EXISTS spree_products;
DROP TABLE IF EXISTS spree_stock_items;
DROP TABLE IF EXISTS spree_stock_locations;

CREATE TABLE spree_users (
    id bigint NOT NULL,
    encrypted_password character varying(128),
    password_salt character varying(128),
    email character varying,
    remember_token character varying,
    persistence_token character varying,
    reset_password_token character varying,
    perishable_token character varying,
    sign_in_count integer DEFAULT 0 NOT NULL,
    failed_attempts integer DEFAULT 0 NOT NULL,
    last_request_at timestamp without time zone,
    current_sign_in_at timestamp without time zone,
    last_sign_in_at timestamp without time zone,
    current_sign_in_ip character varying,
    last_sign_in_ip character varying,
    login character varying,
    ship_address_id bigint,
    bill_address_id bigint,
    authentication_token character varying,
    unlock_token character varying,
    locked_at timestamp without time zone,
    reset_password_sent_at timestamp without time zone,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL,
    public_metadata text,
    private_metadata text,
    first_name character varying,
    last_name character varying,
    selected_locale character varying,
    spree_api_key character varying(48),
    remember_created_at timestamp without time zone,
    deleted_at timestamp without time zone,
    confirmation_token character varying,
    confirmed_at timestamp without time zone,
    confirmation_sent_at timestamp without time zone
);

CREATE SEQUENCE spree_users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE spree_users_id_seq OWNED BY spree_users.id;
ALTER TABLE ONLY spree_users ALTER COLUMN id SET DEFAULT nextval('spree_users_id_seq'::regclass);
ALTER TABLE ONLY spree_users ADD CONSTRAINT spree_users_pkey PRIMARY KEY (id);
CREATE UNIQUE INDEX email_idx_unique ON spree_users USING btree (email);
CREATE INDEX index_spree_users_on_bill_address_id ON spree_users USING btree (bill_address_id);
CREATE INDEX index_spree_users_on_deleted_at ON spree_users USING btree (deleted_at);
CREATE INDEX index_spree_users_on_ship_address_id ON spree_users USING btree (ship_address_id);
CREATE INDEX index_spree_users_on_spree_api_key ON spree_users USING btree (spree_api_key);


CREATE TABLE spree_orders (
    id bigint NOT NULL,
    number character varying(32),
    item_total numeric(10,2) DEFAULT 0.0 NOT NULL,
    total numeric(10,2) DEFAULT 0.0 NOT NULL,
    state character varying,
    adjustment_total numeric(10,2) DEFAULT 0.0 NOT NULL,
    user_id bigint,
    completed_at timestamp without time zone,
    bill_address_id bigint,
    ship_address_id bigint,
    payment_total numeric(10,2) DEFAULT 0.0,
    shipment_state character varying,
    payment_state character varying,
    email character varying,
    special_instructions text,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL,
    currency character varying,
    last_ip_address character varying,
    created_by_id bigint,
    shipment_total numeric(10,2) DEFAULT 0.0 NOT NULL,
    additional_tax_total numeric(10,2) DEFAULT 0.0,
    promo_total numeric(10,2) DEFAULT 0.0,
    channel character varying DEFAULT 'spree'::character varying,
    included_tax_total numeric(10,2) DEFAULT 0.0 NOT NULL,
    item_count integer DEFAULT 0,
    approver_id bigint,
    approved_at timestamp without time zone,
    confirmation_delivered integer DEFAULT '0',
    considered_risky integer DEFAULT '0',
    token character varying,
    canceled_at timestamp without time zone,
    canceler_id bigint,
    store_id bigint,
    state_lock_version integer DEFAULT 0 NOT NULL,
    taxable_adjustment_total numeric(10,2) DEFAULT 0.0 NOT NULL,
    non_taxable_adjustment_total numeric(10,2) DEFAULT 0.0 NOT NULL,
    store_owner_notification_delivered integer,
    public_metadata text,
    private_metadata text,
    internal_note text
);

CREATE SEQUENCE spree_orders_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE spree_orders_id_seq OWNED BY spree_orders.id;
ALTER TABLE ONLY spree_orders ALTER COLUMN id SET DEFAULT nextval('spree_orders_id_seq'::regclass);
ALTER TABLE ONLY spree_orders
    ADD CONSTRAINT spree_orders_pkey PRIMARY KEY (id);
CREATE INDEX index_spree_orders_on_approver_id ON spree_orders USING btree (approver_id);
CREATE INDEX index_spree_orders_on_bill_address_id ON spree_orders USING btree (bill_address_id);
CREATE INDEX index_spree_orders_on_canceler_id ON spree_orders USING btree (canceler_id);
CREATE INDEX index_spree_orders_on_completed_at ON spree_orders USING btree (completed_at);
CREATE INDEX index_spree_orders_on_confirmation_delivered ON spree_orders USING btree (confirmation_delivered);
CREATE INDEX index_spree_orders_on_considered_risky ON spree_orders USING btree (considered_risky);
CREATE INDEX index_spree_orders_on_created_by_id ON spree_orders USING btree (created_by_id);
CREATE UNIQUE INDEX index_spree_orders_on_number ON spree_orders USING btree (number);
CREATE INDEX index_spree_orders_on_ship_address_id ON spree_orders USING btree (ship_address_id);
CREATE INDEX index_spree_orders_on_store_id ON spree_orders USING btree (store_id);
CREATE INDEX index_spree_orders_on_token ON spree_orders USING btree (token);
CREATE INDEX index_spree_orders_on_user_id_and_created_by_id ON spree_orders USING btree (user_id, created_by_id);


CREATE TABLE spree_prices (
    id bigint NOT NULL,
    variant_id bigint NOT NULL,
    amount numeric(10,2),
    currency character varying,
    deleted_at timestamp without time zone,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    compare_at_amount numeric(10,2)
);

CREATE SEQUENCE spree_prices_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE spree_prices_id_seq OWNED BY spree_prices.id;
ALTER TABLE ONLY spree_prices ALTER COLUMN id SET DEFAULT nextval('spree_prices_id_seq'::regclass);
ALTER TABLE ONLY spree_prices
    ADD CONSTRAINT spree_prices_pkey PRIMARY KEY (id);
CREATE INDEX index_spree_prices_on_deleted_at ON spree_prices USING btree (deleted_at);
CREATE INDEX index_spree_prices_on_variant_id ON spree_prices USING btree (variant_id);
CREATE INDEX index_spree_prices_on_variant_id_and_currency ON spree_prices USING btree (variant_id, currency);


CREATE TABLE spree_line_items (
    id bigint NOT NULL,
    variant_id bigint,
    order_id bigint,
    quantity integer NOT NULL,
    price numeric(10,2) NOT NULL,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL,
    currency character varying,
    cost_price numeric(10,2),
    tax_category_id bigint,
    adjustment_total numeric(10,2) DEFAULT 0.0,
    additional_tax_total numeric(10,2) DEFAULT 0.0,
    promo_total numeric(10,2) DEFAULT 0.0,
    included_tax_total numeric(10,2) DEFAULT 0.0 NOT NULL,
    pre_tax_amount numeric(12,4) DEFAULT 0.0 NOT NULL,
    taxable_adjustment_total numeric(10,2) DEFAULT 0.0 NOT NULL,
    non_taxable_adjustment_total numeric(10,2) DEFAULT 0.0 NOT NULL,
    public_metadata text,
    private_metadata text
);

CREATE SEQUENCE spree_line_items_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE spree_line_items_id_seq OWNED BY spree_line_items.id;
ALTER TABLE ONLY spree_line_items ALTER COLUMN id SET DEFAULT nextval('spree_line_items_id_seq'::regclass);
ALTER TABLE ONLY spree_line_items
    ADD CONSTRAINT spree_line_items_pkey PRIMARY KEY (id);
CREATE INDEX index_spree_line_items_on_order_id ON spree_line_items USING btree (order_id);
CREATE INDEX index_spree_line_items_on_tax_category_id ON spree_line_items USING btree (tax_category_id);
CREATE INDEX index_spree_line_items_on_variant_id ON spree_line_items USING btree (variant_id);


CREATE TABLE spree_tax_categories (
    id bigint NOT NULL,
    name character varying,
    description character varying,
    is_default integer DEFAULT '0',
    deleted_at timestamp without time zone,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL,
    tax_code character varying
);

CREATE SEQUENCE spree_tax_categories_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE spree_tax_categories_id_seq OWNED BY spree_tax_categories.id;
ALTER TABLE ONLY spree_tax_categories ALTER COLUMN id SET DEFAULT nextval('spree_tax_categories_id_seq'::regclass);
ALTER TABLE ONLY spree_tax_categories
    ADD CONSTRAINT spree_tax_categories_pkey PRIMARY KEY (id);
CREATE INDEX index_spree_tax_categories_on_deleted_at ON spree_tax_categories USING btree (deleted_at);
CREATE INDEX index_spree_tax_categories_on_is_default ON spree_tax_categories USING btree (is_default);


CREATE TABLE spree_products (
    id bigint NOT NULL,
    name character varying DEFAULT ''::character varying NOT NULL,
    description text,
    available_on timestamp without time zone,
    deleted_at timestamp without time zone,
    slug character varying,
    meta_description text,
    meta_keywords character varying,
    tax_category_id bigint,
    shipping_category_id bigint,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL,
    promotionable integer DEFAULT '1',
    meta_title character varying,
    discontinue_on timestamp without time zone,
    public_metadata text,
    private_metadata text,
    status character varying DEFAULT 'draft'::character varying NOT NULL,
    make_active_at timestamp without time zone
);

CREATE SEQUENCE spree_products_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE spree_products_id_seq OWNED BY spree_products.id;
ALTER TABLE ONLY spree_products ALTER COLUMN id SET DEFAULT nextval('spree_products_id_seq'::regclass);
ALTER TABLE ONLY spree_products
    ADD CONSTRAINT spree_products_pkey PRIMARY KEY (id);
CREATE INDEX index_spree_products_on_available_on ON spree_products USING btree (available_on);
CREATE INDEX index_spree_products_on_deleted_at ON spree_products USING btree (deleted_at);
CREATE INDEX index_spree_products_on_discontinue_on ON spree_products USING btree (discontinue_on);
CREATE INDEX index_spree_products_on_make_active_at ON spree_products USING btree (make_active_at);
CREATE INDEX index_spree_products_on_name ON spree_products USING btree (name);
CREATE INDEX index_spree_products_on_shipping_category_id ON spree_products USING btree (shipping_category_id);
CREATE UNIQUE INDEX index_spree_products_on_slug ON spree_products USING btree (slug);
CREATE INDEX index_spree_products_on_status ON spree_products USING btree (status);
CREATE INDEX index_spree_products_on_status_and_deleted_at ON spree_products USING btree (status, deleted_at);
CREATE INDEX index_spree_products_on_tax_category_id ON spree_products USING btree (tax_category_id);


CREATE TABLE spree_stock_items (
    id bigint NOT NULL,
    stock_location_id bigint,
    variant_id bigint,
    count_on_hand integer DEFAULT 0 NOT NULL,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL,
    backorderable integer DEFAULT '0',
    deleted_at timestamp without time zone,
    public_metadata text,
    private_metadata text
);

CREATE SEQUENCE spree_stock_items_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE spree_stock_items_id_seq OWNED BY spree_stock_items.id;
ALTER TABLE ONLY spree_stock_items ALTER COLUMN id SET DEFAULT nextval('spree_stock_items_id_seq'::regclass);
ALTER TABLE ONLY spree_stock_items
    ADD CONSTRAINT spree_stock_items_pkey PRIMARY KEY (id);
CREATE INDEX index_spree_stock_items_on_backorderable ON spree_stock_items USING btree (backorderable);
CREATE INDEX index_spree_stock_items_on_deleted_at ON spree_stock_items USING btree (deleted_at);
CREATE INDEX index_spree_stock_items_on_stock_location_id ON spree_stock_items USING btree (stock_location_id);
CREATE INDEX index_spree_stock_items_on_variant_id ON spree_stock_items USING btree (variant_id);
CREATE UNIQUE INDEX index_spree_stock_items_unique_without_deleted_at ON spree_stock_items USING btree (variant_id, stock_location_id) WHERE (deleted_at IS NULL);
CREATE INDEX stock_item_by_loc_and_var_id ON spree_stock_items USING btree (stock_location_id, variant_id);
CREATE UNIQUE INDEX stock_item_by_loc_var_id_deleted_at ON spree_stock_items USING btree (stock_location_id, variant_id, deleted_at);


CREATE TABLE spree_stock_locations (
    id bigint NOT NULL,
    name character varying,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL,
    "default" integer DEFAULT '0' NOT NULL,
    address1 character varying,
    address2 character varying,
    city character varying,
    state_id bigint,
    state_name character varying,
    country_id bigint,
    zipcode character varying,
    phone character varying,
    active integer DEFAULT '1',
    backorderable_default integer DEFAULT '0',
    propagate_all_variants integer DEFAULT '0',
    admin_name character varying,
    deleted_at timestamp without time zone
);

CREATE SEQUENCE spree_stock_locations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE spree_stock_locations_id_seq OWNED BY spree_stock_locations.id;
ALTER TABLE ONLY spree_stock_locations ALTER COLUMN id SET DEFAULT nextval('spree_stock_locations_id_seq'::regclass);
ALTER TABLE ONLY spree_stock_locations
    ADD CONSTRAINT spree_stock_locations_pkey PRIMARY KEY (id);
CREATE INDEX index_spree_stock_locations_on_active ON spree_stock_locations USING btree (active);
CREATE INDEX index_spree_stock_locations_on_backorderable_default ON spree_stock_locations USING btree (backorderable_default);
CREATE INDEX index_spree_stock_locations_on_country_id ON spree_stock_locations USING btree (country_id);
CREATE INDEX index_spree_stock_locations_on_deleted_at ON spree_stock_locations USING btree (deleted_at);
CREATE INDEX index_spree_stock_locations_on_propagate_all_variants ON spree_stock_locations USING btree (propagate_all_variants);
CREATE INDEX index_spree_stock_locations_on_state_id ON spree_stock_locations USING btree (state_id);
