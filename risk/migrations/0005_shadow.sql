-- 2b's shadow score (design.md section 2 and section 8; D39, D58c, D58e). Both columns are
-- nullable: with shadow scoring off, or when the model fails, a decision is stored without a
-- score. A score is one number from 0 to 1 (NaN fails the check, since Postgres orders NaN above
-- every number), and only a named model gives one.
alter table decisions
    add column shadow_score double precision,
    add column shadow_model text,
    add constraint decisions_shadow_score_range check (
        shadow_score is null or (shadow_score >= 0 and shadow_score <= 1)
    ),
    add constraint decisions_shadow_score_model check (
        shadow_score is null or shadow_model is not null
    );
