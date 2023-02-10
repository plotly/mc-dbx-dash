# Molson Coors Streamlines Supply Planning Workflows with Databricks & Plotly Dash

## Authors

Sachin Seth (Solutions Architect, Lakeside Analytics)

## Overview

***You must use the dev build if you want to try out SQL Alchemy before its released officially as a part of Databricks SQL connector for python:

pip install databricks-sql-connector==2.2.dev1675985183

Python coders connecting Plotly Dash analytics web applications to Databricks have been well served by the Databricks SQL connector for Python, which exhibits good performance and overall ease of use.

However, as requirements scale in size and complexity — and where both read AND write-back workflows are envisioned — object-relational mapping (ORM) capabilities are superior.

The Databricks SQL connector for Python will soon integrate Databricks SQL Alchemy Engine capabilites facilitating a two-way bridge which connects Plotly Dash apps with Databricks Warehouses. Users will not only be able to read data from the warehouse to the Dash app, but also effectively write back data from the Dash App to the Databricks warehouse .

In this article, we walked through building a simple database on a Databricks SQL, then performing some changes on the data and finally write the modified DataFrame back to the Databricks database.
