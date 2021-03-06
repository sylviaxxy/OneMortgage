/* SQL scripts to create table in PostgreSQL db */

CREATE TABLE loan_contract(
    loan_seq_no text,
    agency_id text,
    first_payment_date date,
    first_time_homebuyer_flag text;
    mip numeric,
    original_loan_term numeric,
    original_cltv numeric,
    original_dti numeric,
    original_upb numeric,
    original_ltv numeric,
    original_interest_rate numeric,
    channel text,
    product_type text,
    loan_purpose text
);

CREATE TABLE monthly_performance(
    loan_seq_no text,
    report_period data,
    servicer_name text,
    cur_interest_rate numeric,
    cur_actual_upb numeric,
    loan_age integer,
    mon_to_maturity integer,
    adjusted_mon_to_maturity integer,
    maturity_date date,
    msa text,
    cur_delinquency text,
    modification text,
    zero_balance_code text,
    zero_balance_date date,
    last_paid_installment_date date,
    foreclosure_date date,
    disposition_date date,
    foreclosure_costs numeric,
    property_preservation_repair_costs numeric,
    asset_recovery_costs numeric,
    miscellaneous_expenses numeric
    associated_taxes numeric,
    net_sale_proceeds numeric,
    credit_enhancement_proceeds numeric,
    repurchase_make_whole_proceeds numeric,
    other_foreclousure_proceeds numeric,
    non_interest_bearing_upb numeric,
    principal_forgiveness_amount numeric,
    repurchase_make_whole_proceeds_flag text,
    foreclousure_principle_write_off_amount numeric,
    servicing_activity_indicator text
);

CREATE TABLE raw_msa_county_mappings (
    cbsa_code integer,
    msad_code integer,
    csa_code integer,
    cbsa_name varchar,
    msa_type varchar,
    msad_name varchar,
    state varchar,
    state_fips integer,
    county_fips integer,
    county_type varchar,
    state_abbreviation varchar
);

CREATE TABLE properties (
);



CREATE TABLE borrowers (
);