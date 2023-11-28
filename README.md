<p align="center">
  <a href="https://devoteam.com"><img src="https://upload.wikimedia.org/wikipedia/commons/thumb/7/79/Dev_logo_rgb.png/640px-Dev_logo_rgb.png" alt="Devoteam"></a>
</p>

# Databricks Champion Demo #2
| | |
| --- | --- |
| Tests | [![CI - Test](https://github.com/ggkenios/databricks-champion-two/actions/workflows/cicd.yml/badge.svg)](https://github.com/ggkenios/databricks-champion-two/actions/workflows/cicd.yml) |
<br>

## Table of Contents
- [What it does](#what-it-does)
- [Requirements](#requirements)
<br>

## What it does
A simple implementation of a CI/CD pipeline for Databricks using **`GitHub Actions`** & **`Databricks Workflows`**. <br>
The pipeline is triggered when a new commit is pushed to selected branches. <br>
<br>
The pipeline consists of the following steps:
* **Push** <br>
    Pushes the code to the Databricks workspace and runs 
    - **`Unit Test`** on databricks worflows 
    - **`Integration Test`** on databricks worflows or DLT

* **Release** <br>
    If the tests pass on the staging environment, the code is released to the production environment.
<br>

## Requirements
* **`Databricks workspace`**
* **`GitHub secrets`** for:
    * Databricks URL
    * Databricks token
<br>

## Inspired By
https://github.com/alexott/dlt-files-in-repos-demo
