
/*
    ratio curve as a function of minutes
    number of minutes required to encompass OTP capacity that covers all patients
    the question is: where is the excess capacity? where are the underserved patients?
    * *global* time radius required for a given location to absorb all its capacity?
    * *global* time radius required for patients to all be served?
    but I also want some sense of VOLUME.
    Kind of a general question:
    * how bad is a problem is it? how large a fraction of the global problem is it?

    * ORTools global optimization?
*/

with otps_capacity_by_zip as (
    select
        p.address_zip_code AS zip_code,
        sum(current_certified_capacity) AS otp_capacity
    from programs_analysis
        join programs as p using (program_number)
    where p.program_category = 'Opioid Treatment Program'
    group by 1
),
patients_by_zip as (
    select
        patient_zip_code AS zip_code,
        sum(coalesce(total_admissions, 3)) as ct_patients
    from program_admissions_2019
        join programs as p using (program_number)
    where p.program_category = 'Opioid Treatment Program'
    group by 1
),
travel_times_from_zip as (
    select
        from_zip_code as zip_code,
        min(case when otp_capacity is not null then (travel_time / 60)::int end) as nearest_otp_minutes,
        sum(case when (travel_time / 60) < 15 then otp_capacity end) 
            as ct_otp_capacity_within_15,
        sum(case when (travel_time / 60) < 30 then otp_capacity end) 
            as ct_otp_capacity_within_30
    from zip_code_distances
        left join otps_capacity_by_zip c on to_zip_code = c.zip_code
    group by 1
),
travel_times_to_zip as (
    select
        to_zip_code as zip_code,
        sum(case when (travel_time / 60) < 15 then ct_patients end) 
            as ct_patients_within_15,
        sum(case when (travel_time / 60) < 30 then ct_patients end) 
            as ct_patients_within_30
    from zip_code_distances
        left join patients_by_zip as p on from_zip_code = p.zip_code
    group by 1
)
select 
    zip_code,
    coalesce(c.otp_capacity, 0) as "OTP Capacity (2019)",
    coalesce(p.ct_patients, 0) as "Estimated Patients",
    nearest_otp_minutes as "Minutes to Nearest OTP",
    ct_otp_capacity_within_15 as "OTP Capacity within 15 Minute Drive",
    ct_patients_within_15 as "OTP Patients within 15 Minute Drive",
    ct_patients_within_15::float / ct_otp_capacity_within_15 as "Ratio of Patients to Capacity within 15 Minute Drive",
    ct_otp_capacity_within_30 as "OTP Capacity within 30 Minute Drive",
    ct_patients_within_30 as "OTP Patients within 30 Minute Drive",
    ct_patients_within_30::float / ct_otp_capacity_within_30 as "Ratio of Patients to Capacity within 30 Minute Drive"

from zip_codes z
    left join otps_capacity_by_zip as c using (zip_code)
    left join patients_by_zip as p using (zip_code)
    left join travel_times_from_zip tf using (zip_code)
    left join travel_times_to_zip tt using (zip_code)
