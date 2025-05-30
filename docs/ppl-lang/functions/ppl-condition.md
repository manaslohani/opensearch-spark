## PPL Condition Functions

### `ISNULL`

**Description**

`isnull(field)` return true if field is null.

**Argument type:**
 - all the supported data type.
 - Return type: **BOOLEAN**

Example:

    os> source=accounts | eval result = isnull(employer) | fields result, employer, firstname
    fetched rows / total rows = 4/4
    +----------+------------+-------------+
    | result   | employer   | firstname   |
    |----------+------------+-------------|
    | False    | Pyrami     | Amber       |
    | False    | Netagy     | Hattie      |
    | False    | Quility    | Nanette     |
    | True     | null       | Dale        |
    +----------+------------+-------------+

### `ISNOTNULL`

**Description**

`isnotnull(field)` return true if field is not null.

**Argument type:**
 - all the supported data type.
 - Return type: **BOOLEAN**

Example:

    os> source=accounts | where not isnotnull(employer) | fields account_number, employer
    fetched rows / total rows = 1/1
    +------------------+------------+
    | account_number   | employer   |
    |------------------+------------|
    | 18               | null       |
    +------------------+------------+

### `EXISTS`

    os> source=accounts | where isnull(email) | fields account_number, email
    fetched rows / total rows = 1/1
    +------------------+---------+
    | account_number   | email   |
    |------------------+---------|
    | 13               | null    |
    +------------------+---------+

### `IFNULL`

**Description**

`ifnull(field1, field2)` return field2 if field1 is null.

**Argument type:**
 - all the supported data type, (NOTE : if two parameters has different type, you will fail semantic check.)
 - Return type: **any**

Example:

    os> source=accounts | eval result = ifnull(employer, 'default') | fields result, employer, firstname
    fetched rows / total rows = 4/4
    +----------+------------+-------------+
    | result   | employer   | firstname   |
    |----------+------------+-------------|
    | Pyrami   | Pyrami     | Amber       |
    | Netagy   | Netagy     | Hattie      |
    | Quility  | Quility    | Nanette     |
    | default  | null       | Dale        |
    +----------+------------+-------------+

### `NULLIF`

**Description**

`nullif(field1, field2)` return null if two parameters are same, otherwise return field1.

**Argument type:**

 - all the supported data type, (NOTE : if two parameters has different type, if two parameters has different type, you will fail semantic check)
 - Return type: **any**

Example:

    os> source=accounts | eval result = nullif(employer, 'Pyrami') | fields result, employer, firstname
    fetched rows / total rows = 4/4
    +----------+------------+-------------+
    | result   | employer   | firstname   |
    |----------+------------+-------------|
    | null     | Pyrami     | Amber       |
    | Netagy   | Netagy     | Hattie      |
    | Quility  | Quility    | Nanette     |
    | null     | null       | Dale        |
    +----------+------------+-------------+


### `ISNULL`

**Description**

`isnull(field1, field2)` return null if two parameters are same, otherwise return field1.

**Argument type:** 
 - all the supported data type
 - Return type: **any**

Example:

    os> source=accounts | eval result = isnull(employer) | fields result, employer, firstname
    fetched rows / total rows = 4/4
    +----------+------------+-------------+
    | result   | employer   | firstname   |
    |----------+------------+-------------|
    | False    | Pyrami     | Amber       |
    | False    | Netagy     | Hattie      |
    | False    | Quility    | Nanette     |
    | True     | null       | Dale        |
    +----------+------------+-------------+

### `IF`

**Description**

`if(condition, expr1, expr2)` return expr1 if condition is true, otherwiser return expr2.

**Argument type:**

 - all the supported data type, (NOTE : if expr1 and expr2 are different type,  you will fail semantic check
 - Return type: **any**

Example:

    os> source=accounts | eval result = if(true, firstname, lastname) | fields result, firstname, lastname
    fetched rows / total rows = 4/4
    +----------+-------------+------------+
    | result   | firstname   | lastname   |
    |----------+-------------+------------|
    | Amber    | Amber       | Duke       |
    | Hattie   | Hattie      | Bond       |
    | Nanette  | Nanette     | Bates      |
    | Dale     | Dale        | Adams      |
    +----------+-------------+------------+

    os> source=accounts | eval result = if(false, firstname, lastname) | fields result, firstname, lastname
    fetched rows / total rows = 4/4
    +----------+-------------+------------+
    | result   | firstname   | lastname   |
    |----------+-------------+------------|
    | Duke     | Amber       | Duke       |
    | Bond     | Hattie      | Bond       |
    | Bates    | Nanette     | Bates      |
    | Adams    | Dale        | Adams      |
    +----------+-------------+------------+

    os> source=accounts | eval is_vip = if(age > 30 AND isnotnull(employer), true, false) | fields is_vip, firstname, lastname
    fetched rows / total rows = 4/4
    +----------+-------------+------------+
    | is_vip   | firstname   | lastname   |
    |----------+-------------+------------|
    | True     | Amber       | Duke       |
    | True     | Hattie      | Bond       |
    | False    | Nanette     | Bates      |
    | False    | Dale        | Adams      |
    +----------+-------------+------------+

### `ISPRESENT`

**Description**

`ispresent(field)` returns true if the field exists.

**Argument type:**

- all the supported data type.
- Return type: **BOOLEAN**

Example:

    os> source=accounts | eval result = ispresent(employer) | fields result, employer, firstname
    fetched rows / total rows = 4/4
    +----------+------------+-------------+
    | result   | employer   | firstname   |
    |----------+------------+-------------|
    | True     | Pyrami     | Amber       |
    | True     | Netagy     | Hattie      |
    | True     | Quility    | Nanette     |
    | False    | null       | Dale        |
    +----------+------------+-------------+

### `ISBLANK`

**Description**

`isblank(field)` returns true if the field is missing, an empty string, or contains only white space.

**Argument type:**

- all the supported data type.
- Return type: **BOOLEAN**

Example:

    os> source=accounts | eval temp = ifnull(employer, '   ') | eval `isblank(employer)` = isblank(employer), `isblank(temp)` = isblank(temp) | fields `isblank(temp)`, temp, `isblank(employer)`, employer
    fetched rows / total rows = 4/4
    +----------------+------------+--------------------+------------+
    | isblank(temp)  | temp       | isblank(employer)  | employer   |
    |----------------+------------+--------------------+------------|
    | False          | Pyrami     | False              | Pyrami     |
    | False          | Netagy     | False              | Netagy     |
    | False          | Quility    | False              | Quility    |
    | True           |            | True               | null       |
    +----------------+------------+--------------------+------------+

### `ISEMPTY`

**Description**

`isempty(field)` returns true if the field is missing or is an empty string.

**Argument type:**

- all the supported data type.
- Return type: **BOOLEAN**

Example:

    os> source=accounts | eval temp = ifnull(employer, '   ') | eval `isempty(employer)` = isempty(employer), `isempty(temp)` = isempty(temp) | fields `isempty(temp)`, temp, `isempty(employer)`, employer
    fetched rows / total rows = 4/4
    +----------------+------------+--------------------+------------+
    | isempty(temp)  | temp       | isempty(employer)  | employer   |
    |----------------+------------+--------------------+------------|
    | False          | Pyrami     | False              | Pyrami     |
    | False          | Netagy     | False              | Netagy     |
    | False          | Quility    | False              | Quility    |
    | False          |            | True               | null       |
    +----------------+------------+--------------------+------------+
