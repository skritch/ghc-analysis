
{% macro categorize_arrest(offense_description, pd_description, offense_level) %}
case 
when {{ offense_description }} in (
        'BURGLAR''S TOOLS', 'BURGLARY', 'CRIMINAL TRESPASS', 
        'CRIMINAL MISCHIEF & RELATED OF', 'CRIMINAL MISCHIEF & RELATED OFFENSES',
        'GRAND LARCENY', 'GRAND LARCENY OF MOTOR VEHICLE', 
        'OTHER OFFENSES RELATED TO THEF', 'OTHER OFFENSES RELATED TO THEFT', 
        'PETIT LARCENY', 'POSSESSION OF STOLEN PROPERTY', 
        'POSSESSION OF STOLEN PROPERTY 5', 'THEFT OF SERVICES', 'THEFT-FRAUD'
    )
    then 'Property'
when {{ offense_description }} in (
        'DANGEROUS WEAPONS', 'DISORDERLY CONDUCT', 'HARASSMENT', 'HARRASSMENT 2', 
        'OFF. AGNST PUB ORD SENSBLTY &', 'OFF. AGNST PUB ORD SENSBLTY & RGHTS TO PRIV', 
        'OFFENSES AGAINST PUBLIC SAFETY', 'ALCOHOLIC BEVERAGE CONTROL LAW', 
        'FORCIBLE TOUCHING', 'JOSTLING', 'OFFENSES AGAINST THE PERSON'
    )
    then 'Disorder'
when {{ offense_description }} in (
        'DANGEROUS DRUGS', 'LOITERING FOR DRUG PURPOSES', 
        'CANNABIS RELATED OFFENSES', 'UNDER THE INFLUENCE, DRUGS'
    )
    or ({{ pd_description }} in ('MARIJUANA, POSSESSION'))
    then 'Drugs'
when {{ offense_description }} in (
        'MOVING INFRACTIONS', 'OTHER TRAFFIC INFRACTION', 
        'UNAUTHORIZED USE OF A VEHICLE', 'UNAUTHORIZED USE OF A VEHICLE 3 (UUV)', 
        'VEHICLE AND TRAFFIC LAWS', 'INTOXICATED & IMPAIRED DRIVING', 
        'INTOXICATED/IMPAIRED DRIVING', 'HOMICIDE-NEGLIGENT-VEHICLE'
    )
    then 'Vehicle'
when {{ offense_description }} in (
        'FELONY ASSAULT', 'ARSON', 'KIDNAPPING', 'KIDNAPPING & RELATED OFFENSES', 
        'MURDER & NON-NEGL. MANSLAUGHTE', 'MURDER & NON-NEGL. MANSLAUGHTER', 
        'RAPE', 'ROBBERY',
        'HOMICIDE-NEGLIGENT,UNCLASSIFIE', 'HOMICIDE-NEGLIGENT,UNCLASSIFIED',
        'ASSAULT 3 & RELATED OFFENSES', 'FELONY SEX CRIMES', 
        'KIDNAPPING AND RELATED OFFENSES'
    )
    or ({{ offense_description }} in ('SEX CRIMES') and {{ offense_level }} = 'F')
    then 'Major'
else 'Other/Unknown'
end
{% endmacro %}