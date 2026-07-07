SELECT
    course_title,
    category,
    difficulty_level,
    instructor_name,
    total_enrolled,
    total_completed,
    completion_rate_pct,
    dropout_rate_pct,
    CASE
        WHEN completion_rate_pct >= 60 THEN 'High Completion'
        WHEN completion_rate_pct >= 40 THEN 'Moderate'
        ELSE 'At Risk'
    END AS completion_flag
FROM gold_course_completion
ORDER BY completion_rate_pct DESC;


SELECT
    learner_id,
    learner_name,
    course_id,
    course_title,
    COUNT(*) AS enrolment_count,
    MAX(attempts) AS max_attempts
FROM silver_enrolments
GROUP BY learner_id, learner_name, course_id, course_title
HAVING COUNT(*) > 1
ORDER BY enrolment_count DESC;


SELECT
    instructor_name,
    category,
    course_title,
    total_enrolled,
    course_completion_rate,
    DENSE_RANK() OVER (
        PARTITION BY category
        ORDER BY course_completion_rate DESC
    ) AS rank_in_category
FROM (
    SELECT
        s.instructor_name,
        s.category,
        s.course_title,
        COUNT(s.enrolment_id) AS total_enrolled,
        ROUND(
            100.0 * SUM(CASE WHEN s.status = 'Completed' THEN 1 ELSE 0 END)
            / COUNT(s.enrolment_id), 2
        ) AS course_completion_rate
    FROM silver_enrolments s
    GROUP BY s.instructor_name, s.category, s.course_title
) ranked
ORDER BY category, rank_in_category;


WITH ranked_enrolments AS (
    SELECT
        enrolment_id,
        learner_id,
        learner_name,
        course_id,
        course_title,
        enrol_date,
        status,
        progress_pct,
        attempts,
        assessment_score,
        ROW_NUMBER() OVER (
            PARTITION BY learner_id, course_id
            ORDER BY enrol_date DESC
        ) AS rn
    FROM silver_enrolments
)
SELECT
    enrolment_id,
    learner_id,
    learner_name,
    course_id,
    course_title,
    enrol_date,
    status,
    progress_pct,
    attempts,
    assessment_score,
    CASE WHEN attempts >= 2 THEN 'Re-enrolment' ELSE 'First Attempt' END AS enrolment_type
FROM ranked_enrolments
WHERE rn = 1
ORDER BY attempts DESC;


SELECT
    course_title,
    category,
    difficulty_level,
    total_assessed,
    avg_score,
    pass_rate_pct,
    avg_attempts,
    CASE
        WHEN pass_rate_pct < 50 THEN 'High Difficulty — Bottleneck'
        WHEN pass_rate_pct < 70 THEN 'Moderate Difficulty'
        ELSE 'Accessible'
    END AS difficulty_flag
FROM gold_assessment_performance
ORDER BY pass_rate_pct ASC;


SELECT
    learner_id,
    learner_name,
    city,
    subscription_type,
    course_title,
    category,
    progress_pct,
    last_activity_date,
    days_since_activity,
    engagement_status
FROM gold_learner_engagement
WHERE engagement_status = 'Disengaged'
ORDER BY days_since_activity DESC;


SELECT
    rank_by_completion,
    instructor_name,
    total_courses,
    total_learners,
    avg_completion_rate_pct,
    avg_assessment_score,
    avg_feedback_rating,
    performance_tier
FROM gold_instructor_effectiveness
ORDER BY rank_by_completion
LIMIT 5;


SELECT
    category,
    COUNT(DISTINCT course_id)    AS total_courses,
    SUM(total_enrolled)          AS total_enrolled,
    SUM(total_completed)         AS total_completed,
    ROUND(
        100.0 * SUM(total_completed) / SUM(total_enrolled), 2
    )                            AS overall_completion_rate_pct,
    SUM(total_dropped)           AS total_dropped
FROM gold_course_completion
GROUP BY category
ORDER BY total_enrolled DESC;


SELECT
    is_reenrolment,
    learner_outcome,
    COUNT(*) AS learner_count,
    ROUND(AVG(assessment_score), 2) AS avg_score,
    ROUND(AVG(progress_pct), 2) AS avg_progress
FROM gold_dropout_reenrolment
GROUP BY is_reenrolment, learner_outcome
ORDER BY is_reenrolment DESC, learner_count DESC;


SELECT
    city,
    COUNT(DISTINCT learner_id)  AS total_learners,
    COUNT(enrolment_id)         AS total_enrolments,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END)
        / COUNT(enrolment_id), 2
    ) AS completion_rate_pct
FROM silver_enrolments
GROUP BY city
ORDER BY total_learners DESC;
