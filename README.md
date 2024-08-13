# EasyDB

A simple, stupid even, python wrapper around psycopg for loading data into a postgres DB.  Lightweight, open source, and free\* (sort of)

## Motivations

I often run into issues at smaller orgs where they are not quite ready for the full power of a production DB environment but are spending hundreds of work hours poking through G-sheets.  Often these orgs have immature, if not non-existent data ingress processes.  This package could be helpful in those early stages, streamlining your data ingress and getting you setup for a CRM. So easy a data analyst could do it.  

## How to run

First of all you should download [Postgres](https://www.postgresql.org/download/) on your local machine for testing.  I like throwing my credentials in a rc file, I'll leave that up to you.   

clone the repo.  