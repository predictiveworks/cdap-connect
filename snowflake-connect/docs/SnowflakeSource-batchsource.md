
# Snowflake Source

## Description
This is a batch connector plugin to read data records from a Snowflake data warehouse, and to transform
them into structured data flow records.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

## Endpoint Configuration
**Host**: The host of the database.

**Port**: The port of the database.

**Account**: Name of the Snowflake account.

**Region**: The name of the cloud region where the data is geographically stored. The region also
determines where computing resources are provisioned. Default value is 'us-west-2'.

## Data Configuration
**Warehouse Name**: Name of the Snowflake data warehouse to export data to.

**Database Name**: The name of database to export data to.

**Database Schema**: Name of the default schema to use for the specified database once connected,
or an empty string. The specified schema should be an existing schema for which the specified default
role has privileges.

**Table Name**: Name of the database table to import data from.

**Import Query**: The SQL select statement to import data from the database. For example:
select * from <your table name>.

## Authentication
**SSL**: Indicator to determine whether a Snowflake connection must use SSL. Values are 'on' or 'off.
Default value = 'on'

**Username**: Name of a registered username.

**User Role**: Specifies the default access control role to use in the Snowflake session initiated by
the driver. The specified role should be an existing role that has already been assigned to the specified
user for the driver. If the specified role has not already been assigned to the user, the role is not used
when the session is initiated by the driver.

**Authenticator**: Specifies the authenticator to use for verifying user login credentials.
You can set this to one of the following values:
* **snowflake** to use the internal Snowflake authenticator.
* **externalbrowser** to use your web browser to authenticate with Okta, ADFS, or any other
  SAML 2.0-compliant identity provider (IdP) that has been defined for your account.
* **oauth** to authenticate using OAuth. When OAuth is specified as the authenticator, you must
  also set the token parameter to specify the OAuth token.
* **snowflake_jwt** to authenticate using key pair authentication.
* **username_password_mfa** to authenticate with MFA token caching.

The default authenticator is *snowflake*.

**OAuth Token**: Specifies the OAuth token to use for authentication. This parameter is required only when
setting the authenticator parameter to 'oauth'. This field is optional, but either an authentication token
or a user password must be provided.

**Password**: Password of the registered user.