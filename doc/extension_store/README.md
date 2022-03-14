# Proposal for Extension Store Manifest

Example for manifest: [manifest.jsonc](./manifest.jsonc)

## Variable Replacements

* Variables placeholders use the `"My variable 'variableName' has value '${variableName}'"` syntax
* Naming conventions:
  * Variables prompted to users are written in `lowerCamelCase`
* Predefined variables:
  * `EXTENSION_ID`, `EXTENSION_VERSION`
    * Used in SQL scripts to make object names (e.g. for schemas, adapter scripts, ...) unique for this extension version, e.g. `CREATE OR REPLACE CONNECTION "${EXTENSION_ID}_${EXTENSION_VERSION}_POSTGRES_CONNECTION" TO ...`
    * This allows auto-generating object names, so the user doesn't need to configure adapter schema and adapter script names.
    * Alternative: Object names could be hard coded in the manifest, but this would require updating the scripts for new extension versions.
* Variables prompted to users are defined in the `parameters` section. The Extension Store asks the user to input values for all parameters.
* Variable placeholders in strings (e.g. BucketFS paths or SQL scripts) are expanded before using them.

## Configuration

Global configuration defined by the system admin:
* `BFS_SERVICE`
* `BUCKET`

## BucketFS Storage

* To avoid conflicts, each extension stores its files in a unique path, e.g. `/bucketfs/<service>/<bucket>/extensions/<extension-id>/<extension-version>/`.
* This allows installing multiple versions of the same extension.

## Information to Store

The Extension management component on the cluster must store the following information for an installed extension:

* Store the complete manifest.
    * Required to detect if parameters are added/removed in newer versions.
* For each configuration:
    * ID and version
        * Required to detect if an older version is already installed
        * Required to detect if the user tries to install another VS using the same configuration
    * Path to installed libraries and configuration files on BucketFS for this extension
        * All files for an extension are stored in a common prefix containing extension ID and version
        * The path might change in later versions of the extension store, so we should store it.
    * Parameters with `scope=configuration`
        * Required when creating a new VS using the same configuration.
        * Cannot be changed during update. Maybe hide them?
* For each Connection / Virtual Schema:
    * ID and version of the configuration
        * Required to check if another VS uses the same configuration
    * Parameters with `scope=connection` that the user entered during the installation process, required for updating the values.
    * **Don't store** values for fields with `type=password`.
        * User has to enter a new password when updating.

## Installation Process

1. If no other VS uses this configuration or the configuration was not installed before:
    1. Ask user to enter values for all parameters with `scope=configuration`
    1. If `licenseAgreementRequired=true` (usually for JDBC drivers): show the license and ask user to agree
    1. Download all artifacts in the `bucketFsUpload`
    1. Upload the artifacts to BucketFS, using the specified file names
    1. Run the `create.configuration` SQL statements
1. Ask user to enter values for all parameters with `scope=connection`
1. Run the `create.connection` SQL statements.

## Update Process

1. Check if there is a newer version of the configuration. If yes:
    1. Ask user to confirm the license again (usually the JDBC driver license)
    1. Download new files
    1. Install new files to BucketFS
    1. Run scripts from `update.configuration`
1. Let user enter new values for all parameters with `scope=connection`
1. Run `update.connection` SQL statements.

## Delete Process

1. Run `drop.connection` SQL statements.
1. Check if the extension version is still used (e.g. in other VS). If no other VS uses this configuration:
    1. Run `drop.configuration` SQL statements.
    1. Delete all files for the extension prefix from BucketFS

## Open Points

* Current JSON structure and property names is just a draft and open for discussion!
* What happens if the Adapter Schema or other resources already exists?
* Simplify process by auto-generating names, e.g. `connectionName`, `adapterScriptName`.
* How to detect if another extension / version already has installed some files?
    * All files for an extension and version are stored in the same prefix, so there are no conflicts.
* How to handle when parameters with `scope=configuration` are added in newer versions?
    * Don't allow this? Prompt the user for new values?
* Allow renaming the VIRTUAL SCHEMA by updating?
    * We need to know the "old" name of the schema to delete it
    * Easy solution: user has to completely delete the VS and create it from scratch
* How to handle rollbacks in case installation fails?
* Revert to previous version if an upgrade fails?
* How to detect if an adapter is still used when trying to delete it?
    * Maybe configure a script that checks if it is still in use.
* Idea: Don't do updates of existing extensions, just install the new version and keep the old one.
    * Upgrading an existing VS would require deleting and re-installing?
    * This requires that the user has to re-enter the credentials.
    * Alternative solution: re-use the existing connection, but it's hard to decide which resources to delete and which to keep.
* Some installation scripts need a different mechanism:
    > Our python extensions, usually, bring a whole language container. For that, we need to add a new language via alter session. This means, we first need to fetch the current script_languages string via `SELECT * FROM SYS.EXA_PARAMETERS WHERE ...` and then append our new container. Maybe, this has to be it own mechanism.
    https://docs.exasol.com/db/latest/database_concepts/udf_scripts/adding_new_packages_script_languages.htm
* Create a JSON Schema for the manifest once the structure is finalized.
* To check if an extension / adapter is still required or can be deleted we could store the resources (e.g. `ADAPTER SCRIPT`, `SCHEMA`, `CONNECTION`, ...) and check if they still exists.
* To make DB object names (e.g. schema, adapter scripts etc.) unique we need to add extension ID and version to the name.