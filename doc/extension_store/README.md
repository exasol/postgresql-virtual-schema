# Proposal for Extension Store Manifest

Example for manifest: [manifest.jsonc](./manifest.jsonc)

## Variable Replacements

* Variables placeholders use the `"My variable 'variableName' has value '${variableName}'"` syntax
* Naming conventions:
  * Variables prompted to users are written in `lowerCamelCase`
  * Global constants defined by the system admin:
    * `BFS_SERVICE`
    * `BUCKET`
* User defined variables are defined in the `parameters` section. The Extension Store asks the user to input values for all parameters.
* Variable placeholders in strings (e.g. BucketFS paths or SQL scripts) are expanded before using them.

## Parameters to Store

The Extension management component on the cluster must store the following information for an installed VS:

* For each adapter:
    * ID and version
        * Required to detect if an older version is already installed
        * Required to detect if the user tries to install another VS using the same adapter
    * Paths to installed libraries and configuration files on BucketFS
        * File names might change in newer versions.
        * Needed to delete files from BucketFS during update/delete
    * Parameters with `scope=adapter`
        * Required when creating a new VS using the same adapter.
        * Cannot be changed during update. Maybe hide them?
* For each Connection / Virtual Schema:
    * ID and version of the adapter
        * Required to check if another VS uses the same adapter
    * Parameters with `scope=connection` that the user entered during the installation process, required for updating the values.
    * **Don't store** values for fields with `type=password`.
        * User has to enter a new password when updating.

## Installation Process

1. If no other VS uses this adapter / adapter was not installed before:
    1. Ask user to enter values for all parameters with `scope=configuration`
    1. If `licenseAgreementRequired=true`: show the license and asks user to agree
    1. Download all artifacts in the `bucketFsUpload`
    1. Upload the artifacts to BucketFS, using the specified path names
    1. Run the `create.adapter` SQL statements
1. Ask user to enter values for all parameters with `scope=connection`
1. Run the `create.connection` SQL statements.

## Update Process

1. Check if there is a newer version of the adapter. If yes:
    1. Ask user to confirm the license again
    1. Download new JARs
    1. Delete old JARs from BucketFS
    1. Install new JARs to BucketFS
    1. Run scripts from `update.adapter`
1. Let user enter new values for all parameters with `scope=connection`
1. Run `update.connection` SQL statements.

## Delete Process

1. Run `drop.connection` SQL statements.
1. If no other VS uses this adapter:
    1. Run `drop.adapter` SQL statements.
    1. Delete JARs from BucketFS (using stored paths)

## Open Points

* Current JSON structure and property names is just a draft and open for discussion!
* What happens if the Adapter Schema or other resources already exists?
* Simplify process by auto-generating names, e.g. `connectionName`, `adapterScriptName`.
* How to detect if another VS already has installed some JARs?
* How to handle when parameters with `scope=adapter` are added in newer versions?
    * Don't allow this? Prompt the user for new values?
* Allow renaming the VIRTUAL SCHEMA by updating?
    * We need to know the "old" name of the schema to delete it
    * Easy solution: user has to completely delete the VS and create it from scratch