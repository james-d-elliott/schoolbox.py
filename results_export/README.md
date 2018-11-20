# Results Export

## Overview

Example of how to export data from Schoolbox Resutls to CSV (Excel Compatible) and calculate averages etc.

We use a scale score for most of our results which is a grade in the format of a decimal in increments of 0.1 from 2.0 to 12.0 where 12.0 indicates work was completed at a level expected of a year 12 student in the final week of school. I used a primitive way to detect this (just regex) however you can use your SQL statement to do this if you desire which is technically more accurate.

## Required Modules

sshtunnel
MySQLdb
yaml

## Notes

Needs/Expects a students.csv in the format of ID,Year,CampusCode,House,Name without the header.

## Plans

Make this more transportable so others can use it.
Make the connection method more configurable.
