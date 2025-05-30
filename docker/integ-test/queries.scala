{
    try {
        spark.sql("describe myglue_test.default.http_logs")
        throw new Error
    } catch {
        case e: Exception => null
    }

    try {
        spark.sql("describe `myglue_test`.`default`.`http_logs`")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.http_logs | dedup 1 status | fields @timestamp, clientip, status, size | head 10")

    spark.sql("source = myglue_test.default.http_logs | dedup status, size | head 10")

    spark.sql("source = myglue_test.default.http_logs | dedup 1 status keepempty=true | head 10")

    spark.sql("source = myglue_test.default.http_logs | dedup status, size keepempty=true | head 10")

    spark.sql("source = myglue_test.default.http_logs | dedup 2 status | head 10")

    spark.sql("source = myglue_test.default.http_logs | dedup 2 status, size | head 10")

    spark.sql("source = myglue_test.default.http_logs | dedup 2 status, size keepempty=true | head 10")

    try {
        spark.sql("source = myglue_test.default.http_logs | dedup status CONSECUTIVE=true | fields status")
        throw new Error
    } catch {
        case e: Exception => null
    }

    try {
        spark.sql("source = myglue_test.default.http_logs | dedup 2 status, size  CONSECUTIVE=true | fields status")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.http_logs | sort stat | fields @timestamp, clientip, status | head 10")

    try {
        spark.sql("source = myglue_test.default.http_logs | fields @timestamp, notexisted | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    try {
        spark.sql("source = myglue_test.default.nested | fields int_col, struct_col.field1, struct_col2.field1 | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    try {
        spark.sql("source = myglue_test.default.nested | where struct_col2.field1.subfield > 'valueA' | sort int_col | fields int_col, struct_col.field1.subfield, struct_col2.field1.subfield")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.http_logs | fields - @timestamp, clientip, status | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval new_time = @timestamp, new_clientip = clientip | fields - new_time, new_clientip, status | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval new_clientip = lower(clientip) | fields - new_clientip | head 10")

    spark.sql("source = myglue_test.default.http_logs | fields + @timestamp, clientip, status | fields - clientip, status | head 10")

    spark.sql("source = myglue_test.default.http_logs | fields - clientip, status  | fields + @timestamp, clientip, status| head 10")

    spark.sql("source = myglue_test.default.http_logs | where status = 200 | head 10")

    spark.sql("source = myglue_test.default.http_logs | where status != 200 | head 10")

    spark.sql("source = myglue_test.default.http_logs | where size > 0 | head 10")

    spark.sql("source = myglue_test.default.http_logs | where size <= 0 | head 10")

    spark.sql("source = myglue_test.default.http_logs | where clientip = '236.14.2.0' | head 10")

    spark.sql("source = myglue_test.default.http_logs | where size > 0 AND status = 200 OR clientip = '236.14.2.0' | head 100")

    spark.sql("source = myglue_test.default.http_logs | where size <= 0 AND like(request, 'GET%')  | head 10")

    spark.sql("source = myglue_test.default.http_logs status = 200 | head 10")

    spark.sql("source = myglue_test.default.http_logs size > 0 AND status = 200 OR clientip = '236.14.2.0' | head 100")

    spark.sql("source = myglue_test.default.http_logs size <= 0 AND like(request, 'GET%') | head 10")

    spark.sql("source = myglue_test.default.http_logs substring(clientip, 5, 2) = \"12\" | head 10")

    try {
        spark.sql("source = myglue_test.default.http_logs | where isempty(size)")
        throw new Error
    } catch {
        case e: Exception => null
    }

    try {
        spark.sql("source = myglue_test.default.http_logs | where ispresent(size)")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.http_logs | where isnull(size) | head 10")

    spark.sql("source = myglue_test.default.http_logs | where isnotnull(size) | head 10")

    try {
        spark.sql("source = myglue_test.default.http_logs | where isnotnull(coalesce(size, status)) | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.http_logs | where like(request, 'GET%') | head 10")

    spark.sql("source = myglue_test.default.http_logs | where like(request, '%bordeaux%') | head 10")

    spark.sql("source = myglue_test.default.http_logs | where substring(clientip, 5, 2) = \"12\" | head 10")

    spark.sql("source = myglue_test.default.http_logs | where lower(request) = \"get /images/backnews.gif http/1.0\" | head 10")

    spark.sql("source = myglue_test.default.http_logs | where length(request) = 38 | head 10")

    try {
        spark.sql("source = myglue_test.default.http_logs | where case(status = 200, 'success' else 'failed') = 'success' | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.http_logs | eval h = \"Hello\",  w = \"World\" | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval @h = \"Hello\" | eval @w = \"World\" | fields @timestamp, @h, @w")

    spark.sql("source = myglue_test.default.http_logs | eval newF = clientip | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval newF = clientip | fields clientip, newF | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval f = size | where f > 1 | sort f | fields size, clientip, status | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval f = status * 2 | eval h = f * 2 | fields status, f, h | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval f = size * 2, h = status | stats sum(f) by h")

    spark.sql("source = myglue_test.default.http_logs | eval f = UPPER(request) | eval h = 40 | fields f, h | head 10")

    try {
        spark.sql("source = myglue_test.default.http_logs | eval request = \"test\" | fields request | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    try {
        spark.sql("source = myglue_test.default.http_logs | eval size = abs(size) | where size < 500")
        throw new Error
    } catch {
        case e: Exception => null
    }

    try {
        spark.sql("source = myglue_test.default.http_logs | eval status_string = case(status = 200, 'success' else 'failed') | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.http_logs | eval n = now() | eval t = unix_timestamp(@timestamp) | fields n, t | head 10")

    try {
        spark.sql("source = myglue_test.default.http_logs | eval e = isempty(size) | eval p = ispresent(size) | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    try {
        spark.sql("source = myglue_test.default.http_logs | eval c = coalesce(size, status) | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    try {
        spark.sql("source = myglue_test.default.http_logs | eval c = coalesce(request) | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.http_logs | eval col1 = ln(size) | eval col2 = unix_timestamp(@timestamp) | sort - col1 | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval col1 = 1 | sort col1 | head 4 | eval col2 = 2 | sort - col2 | sort - size | head 2 | fields @timestamp, clientip, col2")

    spark.sql("source = myglue_test.default.mini_http_logs | eval stat = status | where stat > 300 | sort stat | fields @timestamp,clientip,status | head 5")

    spark.sql("source = myglue_test.default.http_logs |  eval col1 = size, col2 = clientip | stats avg(col1) by col2")

    spark.sql("source = myglue_test.default.http_logs | stats avg(size) by clientip")

    spark.sql("source = myglue_test.default.http_logs | eval new_request = upper(request) | eval compound_field = concat('Hello ', if(like(new_request, '%bordeaux%'), 'World', clientip)) | fields new_request, compound_field | head 10")

    spark.sql("source = myglue_test.default.http_logs | stats avg(size)")

    spark.sql("source = myglue_test.default.nested | stats max(int_col) by struct_col.field2")

    spark.sql("source = myglue_test.default.nested | stats distinct_count(int_col)")

    spark.sql("source = myglue_test.default.nested | stats stddev_samp(int_col)")

    spark.sql("source = myglue_test.default.nested | stats stddev_pop(int_col)")

    spark.sql("source = myglue_test.default.nested | stats percentile(int_col)")

    spark.sql("source = myglue_test.default.nested | stats percentile_approx(int_col)")

    spark.sql("source = myglue_test.default.mini_http_logs | stats stddev_samp(status)")

    spark.sql("source = myglue_test.default.mini_http_logs | where stats > 200 | stats percentile_approx(status, 99)")

    spark.sql("source = myglue_test.default.nested | stats count(int_col) by span(struct_col.field2, 10) as a_span")

    spark.sql("source = myglue_test.default.nested | stats avg(int_col) by span(struct_col.field2, 10) as a_span, struct_col2.field2")

    spark.sql("source = myglue_test.default.http_logs | stats sum(size) by span(@timestamp, 1d) as age_size_per_day | sort - age_size_per_day | head 10")

    spark.sql("source = myglue_test.default.http_logs | stats distinct_count(clientip) by span(@timestamp, 1d) as age_size_per_day | sort - age_size_per_day | head 10")

    spark.sql("source = myglue_test.default.http_logs | stats avg(size) as avg_size by status, year | stats avg(avg_size) as avg_avg_size by year")

    spark.sql("source = myglue_test.default.http_logs | stats avg(size) as avg_size by status, year, month | stats avg(avg_size) as avg_avg_size by year, month | stats avg(avg_avg_size) as avg_avg_avg_size by year")

    try {
        spark.sql("source = myglue_test.default.nested | stats avg(int_col) as avg_int by struct_col.field2, struct_col2.field2 | stats avg(avg_int) as avg_avg_int by struct_col2.field2")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.nested | stats avg(int_col) as avg_int by struct_col.field2, struct_col2.field2 | eval new_col = avg_int | stats avg(avg_int) as avg_avg_int by new_col")

    spark.sql("source = myglue_test.default.nested | rare int_col")

    spark.sql("source = myglue_test.default.nested | rare int_col by struct_col.field2")

    spark.sql("source = myglue_test.default.http_logs | rare request")

    spark.sql("source = myglue_test.default.http_logs | where status > 300 | rare request by status")

    spark.sql("source = myglue_test.default.http_logs | rare clientip")

    spark.sql("source = myglue_test.default.http_logs | where status > 300 | rare clientip")

    spark.sql("source = myglue_test.default.http_logs | where status > 300 | rare clientip by day")

    spark.sql("source = myglue_test.default.nested | top int_col by struct_col.field2")

    spark.sql("source = myglue_test.default.nested | top 1 int_col by struct_col.field2")

    spark.sql("source = myglue_test.default.nested | top 2 int_col by struct_col.field2")

    spark.sql("source = myglue_test.default.nested | top int_col")

    try {
        spark.sql("source = myglue_test.default.http_logs | inner join left=l right=r on l.status = r.int_col myglue_test.default.nested | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.http_logs | parse request 'GET /(?<domain>[a-zA-Z]+)/.*' | fields request, domain | head 10")

    spark.sql("source = myglue_test.default.http_logs | parse request 'GET /(?<domain>[a-zA-Z]+)/.*' | top 1 domain")

    spark.sql("source = myglue_test.default.http_logs | parse request 'GET /(?<domain>[a-zA-Z]+)/.*' | stats count() by domain")

    spark.sql("source = myglue_test.default.http_logs | parse request 'GET /(?<domain>[a-zA-Z]+)/.*' | eval a = 1 | fields a, domain | head 10")

    spark.sql("source = myglue_test.default.http_logs | parse request 'GET /(?<domain>[a-zA-Z]+)/.*' | where size > 0 | sort - size | fields size, domain | head 10")

    spark.sql("source = myglue_test.default.http_logs | parse request 'GET /(?<domain>[a-zA-Z]+)/(?<picName>[a-zA-Z]+)/.*' | where domain = 'english' | sort - picName | fields domain, picName | head 10")

    spark.sql("source = myglue_test.default.http_logs | patterns request | fields patterns_field | head 10")

    spark.sql("source = myglue_test.default.http_logs | patterns request | where size > 0 | fields patterns_field | head 10")

    spark.sql("source = myglue_test.default.http_logs | patterns new_field='no_letter' pattern='[a-zA-Z]' request | fields request, no_letter | head 10")

    spark.sql("source = myglue_test.default.http_logs | patterns new_field='no_letter' pattern='[a-zA-Z]' request | stats count() by no_letter")

    try {
        spark.sql("source = myglue_test.default.http_logs | patterns new_field='status' pattern='[a-zA-Z]' request | fields request, status | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    try {
        spark.sql("source = myglue_test.default.http_logs | rename @timestamp as timestamp | head 10")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.default.http_logs | sort size | head 10")

    spark.sql("source = myglue_test.default.http_logs | sort + size | head 10")

    spark.sql("source = myglue_test.default.http_logs | sort - size | head 10")

    spark.sql("source = myglue_test.default.http_logs | sort + size, + @timestamp | head 10")

    spark.sql("source = myglue_test.default.http_logs | sort - size, - @timestamp | head 10")

    spark.sql("source = myglue_test.default.http_logs | sort - size, @timestamp | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval c1 = upper(request) | eval c2 = concat('Hello ', if(like(c1, '%bordeaux%'), 'World', clientip)) | eval c3 = length(request) | eval c4 = ltrim(request) | eval c5 = rtrim(request) | eval c6 = substring(clientip, 5, 2) | eval c7 = trim(request) | eval c8 = upper(request) | eval c9 = position('bordeaux' IN request) | eval c10 = replace(request, 'GET', 'GGG') | fields c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval c1 = unix_timestamp(@timestamp) | eval c2 = now() | eval c3 =DAY_OF_WEEK(@timestamp) | eval c4 =DAY_OF_MONTH(@timestamp) | eval c5 =DAY_OF_YEAR(@timestamp) | eval c6 =WEEK_OF_YEAR(@timestamp) | eval c7 =WEEK(@timestamp) | eval c8 =MONTH_OF_YEAR(@timestamp) | eval c9 =HOUR_OF_DAY(@timestamp) | eval c10 =MINUTE_OF_HOUR(@timestamp) | eval c11 =SECOND_OF_MINUTE(@timestamp) | eval c12 =LOCALTIME() | fields c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12 | head 10")

    spark.sql("source=myglue_test.default.people  | eval c1 = adddate(@timestamp, 1) | fields c1 | head 10")

    spark.sql("source=myglue_test.default.people  | eval c2 = subdate(@timestamp, 1) | fields c2 | head 10")

    spark.sql("source=myglue_test.default.people  | eval c1 = date_add(@timestamp INTERVAL 1 DAY) | fields c1 | head 10")

    spark.sql("source=myglue_test.default.people  | eval c1 = date_sub(@timestamp INTERVAL 1 DAY) | fields c1 | head 10")

    spark.sql("source=myglue_test.default.people | eval `CURDATE()` = CURDATE() | fields `CURDATE()`")

    spark.sql("source=myglue_test.default.people | eval `CURRENT_DATE()` = CURRENT_DATE() | fields `CURRENT_DATE()`")

    spark.sql("source=myglue_test.default.people | eval `CURRENT_TIMESTAMP()` = CURRENT_TIMESTAMP() | fields `CURRENT_TIMESTAMP()`")

    spark.sql("source=myglue_test.default.people | eval `DATE('2020-08-26')` = DATE('2020-08-26') | fields `DATE('2020-08-26')`")

    spark.sql("source=myglue_test.default.people  | eval `DATE(TIMESTAMP('2020-08-26 13:49:00'))` = DATE(TIMESTAMP('2020-08-26 13:49:00')) | fields `DATE(TIMESTAMP('2020-08-26 13:49:00'))`")

    spark.sql("source=myglue_test.default.people  | eval `DATE('2020-08-26 13:49')` = DATE('2020-08-26 13:49') | fields `DATE('2020-08-26 13:49')`")

    spark.sql("source=myglue_test.default.people  | eval `DATE_FORMAT('1998-01-31 13:14:15.012345', 'HH:mm:ss.SSSSSS')` = DATE_FORMAT('1998-01-31 13:14:15.012345', 'HH:mm:ss.SSSSSS'), `DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), 'yyyy-MMM-dd hh:mm:ss a')` = DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), 'yyyy-MMM-dd hh:mm:ss a') | fields `DATE_FORMAT('1998-01-31 13:14:15.012345', 'HH:mm:ss.SSSSSS')`, `DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), 'yyyy-MMM-dd hh:mm:ss a')`")

    spark.sql("source=myglue_test.default.people  | eval `'2000-01-02' - '2000-01-01'` = DATEDIFF(TIMESTAMP('2000-01-02 00:00:00'), TIMESTAMP('2000-01-01 23:59:59')), `'2001-02-01' - '2004-01-01'` = DATEDIFF(DATE('2001-02-01'), TIMESTAMP('2004-01-01 00:00:00')) | fields `'2000-01-02' - '2000-01-01'`, `'2001-02-01' - '2004-01-01'`")

    spark.sql("source=myglue_test.default.people  | eval `DAY(DATE('2020-08-26'))` = DAY(DATE('2020-08-26')) | fields `DAY(DATE('2020-08-26'))`")

    try {
        spark.sql("source=myglue_test.default.people  | eval `DAYNAME(DATE('2020-08-26'))` = DAYNAME(DATE('2020-08-26')) | fields `DAYNAME(DATE('2020-08-26'))`")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source=myglue_test.default.people  | eval `CURRENT_TIMEZONE()` = CURRENT_TIMEZONE() | fields `CURRENT_TIMEZONE()`")

    spark.sql("source=myglue_test.default.people  | eval `UTC_TIMESTAMP()` = UTC_TIMESTAMP() | fields `UTC_TIMESTAMP()`")

    spark.sql("source=myglue_test.default.people  | eval `TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')` = TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00') | eval `TIMESTAMPDIFF(SECOND, timestamp('1997-01-01 00:00:23'), timestamp('1997-01-01 00:00:00'))` = TIMESTAMPDIFF(SECOND, timestamp('1997-01-01 00:00:23'), timestamp('1997-01-01 00:00:00')) | fields `TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')`, `TIMESTAMPDIFF(SECOND, timestamp('1997-01-01 00:00:23'), timestamp('1997-01-01 00:00:00'))`")

    spark.sql("source=myglue_test.default.people  | eval `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')` = TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00') | eval `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')` = TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00') | fields `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')`, `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')`")

    spark.sql(" source = myglue_test.default.http_logs | stats count()")

    spark.sql("source = myglue_test.default.http_logs | stats avg(size) as c1, max(size) as c2, min(size) as c3, sum(size) as c4, percentile(size, 50) as c5, stddev_pop(size) as c6, stddev_samp(size) as c7, distinct_count(size) as c8")

    spark.sql("source = myglue_test.default.http_logs | eval c1 = abs(size) | eval c2 = ceil(size) | eval c3 = floor(size) | eval c4 = sqrt(size) | eval c5 = ln(size) | eval c6 = pow(size, 2) | eval c7 = mod(size, 2) | fields c1, c2, c3, c4, c5, c6, c7 | head 10")

    spark.sql("source = myglue_test.default.http_logs | eval c1 = isnull(request) | eval c2 = isnotnull(request) | eval c3 = ifnull(request,\"Unknown\") | eval c4 = nullif(request,\"Unknown\") | eval c5 = isnull(size) | eval c6 = if(like(request, '%bordeaux%'), 'hello', 'world') | fields c1, c2, c3, c4, c5, c6 | head 10")

    spark.sql("/* this is block comment */ source = myglue_test.tpch_csv.orders | head 1 // this is line comment")

    spark.sql("/* test in tpch q16, q18, q20 */ source = myglue_test.tpch_csv.orders | head 1 // add source=xx to avoid failure in automation")

    spark.sql("/* test in tpch q4, q21, q22 */ source = myglue_test.tpch_csv.orders | head 1")

    spark.sql("/* test in tpch q2, q11, q15, q17, q20, q22 */ source = myglue_test.tpch_csv.orders | head 1")

    spark.sql("/* test in tpch q7, q8, q9, q13, q15, q22 */ source = myglue_test.tpch_csv.orders | head 1")

    spark.sql("/* lots of inner join tests in tpch */ source = myglue_test.tpch_csv.orders | head 1")

    spark.sql("/* left join test in tpch q13 */ source = myglue_test.tpch_csv.orders | head 1")

    spark.sql("source = myglue_test.tpch_csv.orders | right outer join ON c_custkey = o_custkey AND not like(o_comment, '%special%requests%')  myglue_test.tpch_csv.customer| stats count(o_orderkey) as c_count by c_custkey| sort - c_count")

    spark.sql("source = myglue_test.tpch_csv.orders | full outer join ON c_custkey = o_custkey AND not like(o_comment, '%special%requests%')  myglue_test.tpch_csv.customer| stats count(o_orderkey) as c_count by c_custkey| sort - c_count")

    spark.sql("source = myglue_test.tpch_csv.customer| semi join ON c_custkey = o_custkey myglue_test.tpch_csv.orders| where c_mktsegment = 'BUILDING' | sort - c_custkey| head 10")

    spark.sql("source = myglue_test.tpch_csv.customer| anti join ON c_custkey = o_custkey myglue_test.tpch_csv.orders| where c_mktsegment = 'BUILDING' | sort - c_custkey| head 10")

    spark.sql("source = myglue_test.tpch_csv.supplier| where like(s_comment, '%Customer%Complaints%')| join ON s_nationkey > n_nationkey [ source = myglue_test.tpch_csv.nation | where n_name = 'SAUDI ARABIA' ]| sort - s_name| head 10")

    spark.sql("source = myglue_test.tpch_csv.supplier| where like(s_comment, '%Customer%Complaints%')| join [ source = myglue_test.tpch_csv.nation | where n_name = 'SAUDI ARABIA' ]| sort - s_name| head 10")

    spark.sql("source=myglue_test.default.people | LOOKUP myglue_test.default.work_info uid AS id REPLACE department | stats distinct_count(department)")

    spark.sql("source = myglue_test.default.people| LOOKUP myglue_test.default.work_info uid AS id APPEND department | stats distinct_count(department)")

    spark.sql("source = myglue_test.default.people| LOOKUP myglue_test.default.work_info uid AS id REPLACE department AS country | stats distinct_count(country)")

    spark.sql("source = myglue_test.default.people| LOOKUP myglue_test.default.work_info uid AS id APPEND department AS country | stats distinct_count(country)")

    spark.sql("source = myglue_test.default.people| LOOKUP myglue_test.default.work_info uid AS id, name REPLACE department | stats distinct_count(department)")

    spark.sql("source = myglue_test.default.people| LOOKUP myglue_test.default.work_info uid AS id, name APPEND department | stats distinct_count(department)")

    spark.sql("source = myglue_test.default.people| LOOKUP myglue_test.default.work_info uid AS id, name | head 10")

    spark.sql("source = myglue_test.default.people | eval major = occupation | fields id, name, major, country, salary | LOOKUP myglue_test.default.work_info name REPLACE occupation AS major | stats distinct_count(major)")

    spark.sql("source = myglue_test.default.people | eval major = occupation | fields id, name, major, country, salary | LOOKUP myglue_test.default.work_info name APPEND occupation AS major | stats distinct_count(major)")

    spark.sql("source = myglue_test.default.http_logs | eval res = json('{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json('[]') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json(‘{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json('{\"invalid\": \"json\"') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json('[1,2,3]') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json(‘[1,2') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json('[invalid json]') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json('invalid json') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json(null) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_array('this', 'is', 'a', 'string', 'array') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_array() | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_array(1, 2, 0, -1, 1.1, -0.11) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_array('this', 'is', 1.1, -0.11, true, false) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = to_json_string(json_array(1,2,0,-1,1.1,-0.11)) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = array_length(json_array(1,2,0,-1,1.1,-0.11)) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = array_length(json_array()) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_array_length('[]') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_array_length('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_array_length('{\"key\": 1}') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_array_length('[1,2') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = to_json_string(json_object('key', 'string_value')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = to_json_string(json_object('key', 123.45)) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = to_json_string(json_object('key', true)) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = to_json_string(json_object(\"a\", 1, \"b\", 2, \"c\", 3)) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = to_json_string(json_object('key', array())) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = to_json_string(json_object('key', array(1, 2, 3))) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = to_json_string(json_object('outer', json_object('inner', 123.45))) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = to_json_string(json_object(\"array\", json_array(1,2,0,-1,1.1,-0.11))) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | where json_valid(('{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}') | head 1")

    spark.sql("source = myglue_test.default.http_logs | where not json_valid(('{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}') | head 1")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json('{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json('[]')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json(‘{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json('{\"invalid\": \"json\"')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json('[1,2,3]')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json('[1,2')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json('[invalid json]')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json('invalid json')) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_keys(json(null)) | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_extract('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}', '$') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_extract('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}', '$.teacher') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_extract('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}', '$.student') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_extract('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}', '$.student[*]') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_extract('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}', '$.student[0]') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_extract('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}', '$.student[*].name') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_extract('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}', '$.student[1].name') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_extract('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}', '$.student[0].not_exist_key') | head 1 | fields res")

    spark.sql("source = myglue_test.default.http_logs | eval res = json_extract('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}', '$.student[10]') | head 1 | fields res")

    spark.sql("source = myglue_test.default.people | eval array = json_array(1,2,0,-1,1.1,-0.11), result = forall(array, x -> x > 0) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(1,2,0,-1,1.1,-0.11), result = forall(array, x -> x > -10) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(json_object(\"a\",1,\"b\",-1),json_object(\"a\",-1,\"b\",-1)), result = forall(array, x -> x.a > 0) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(json_object(\"a\",1,\"b\",-1),json_object(\"a\",-1,\"b\",-1)), result = exists(array, x -> x.b < 0) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(1,2,0,-1,1.1,-0.11), result = exists(array, x -> x > 0) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(1,2,0,-1,1.1,-0.11), result = exists(array, x -> x > 10) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(1,2,0,-1,1.1,-0.11), result = filter(array, x -> x > 0) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(1,2,0,-1,1.1,-0.11), result = filter(array, x -> x > 10) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(1,2,3), result = transform(array, x -> x + 1) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(1,2,3), result = transform(array, (x, y) -> x + y) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(1,2,3), result = reduce(array, 0, (acc, x) -> acc + x) | head 1 | fields result")

    spark.sql("source = myglue_test.default.people | eval array = json_array(1,2,3), result = reduce(array, 0, (acc, x) -> acc + x, acc -> acc * 10) | head 1 | fields result")

    spark.sql("source=myglue_test.default.people | eval age = salary | eventstats avg(age) | sort id | head 10")

    spark.sql("source=myglue_test.default.people | eval age = salary | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count | sort id | head 10")

    spark.sql("source=myglue_test.default.people | eventstats avg(salary) by country | sort id | head 10")

    spark.sql("source=myglue_test.default.people | eval age = salary | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count by country | sort id | head 10")

    spark.sql("source=myglue_test.default.people | eval age = salary | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as countby span(age, 10) | sort id | head 10")

    spark.sql("source=myglue_test.default.people | eval age = salary | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count by span(age, 10) as age_span, country | sort id | head 10")

    spark.sql("source=myglue_test.default.people | where country != 'USA' | eventstats stddev_samp(salary), stddev_pop(salary), percentile_approx(salary, 60) by span(salary, 1000) as salary_span | sort id | head 10")

    spark.sql("source=myglue_test.default.people | eval age = salary | eventstats avg(age) as avg_age by occupation, country | eventstats avg(avg_age) as avg_state_age by country | sort id | head 10")

    try {
        spark.sql("source=myglue_test.default.people | eventstats distinct_count(salary) by span(salary, 1000) as age_span")
        throw new Error
    } catch {
        case e: Exception => null
    }

    spark.sql("source = myglue_test.tpch_csv.lineitem| where l_shipdate <= subdate(date('1998-12-01'), 90)| stats sum(l_quantity) as sum_qty,    sum(l_extendedprice) as sum_base_price,    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,    avg(l_quantity) as avg_qty,    avg(l_extendedprice) as avg_price,    avg(l_discount) as avg_disc,    count() as count_order   by l_returnflag, l_linestatus| sort l_returnflag, l_linestatus")

    spark.sql("source = myglue_test.tpch_csv.part| join ON p_partkey = ps_partkey myglue_test.tpch_csv.partsupp| join ON s_suppkey = ps_suppkey myglue_test.tpch_csv.supplier| join ON s_nationkey = n_nationkey myglue_test.tpch_csv.nation| join ON n_regionkey = r_regionkey myglue_test.tpch_csv.region| where p_size = 15 AND like(p_type, '%BRASS') AND r_name = 'EUROPE' AND ps_supplycost = [    source = myglue_test.tpch_csv.partsupp    | join ON s_suppkey = ps_suppkey myglue_test.tpch_csv.supplier    | join ON s_nationkey = n_nationkey myglue_test.tpch_csv.nation    | join ON n_regionkey = r_regionkey myglue_test.tpch_csv.region    | where r_name = 'EUROPE'    | stats MIN(ps_supplycost)  ]| sort - s_acctbal, n_name, s_name, p_partkey| head 100")

    spark.sql("source = myglue_test.tpch_csv.customer| join ON c_custkey = o_custkey myglue_test.tpch_csv.orders| join ON l_orderkey = o_orderkey myglue_test.tpch_csv.lineitem| where c_mktsegment = 'BUILDING' AND o_orderdate < date('1995-03-15') AND l_shipdate > date('1995-03-15')| stats sum(l_extendedprice * (1 - l_discount)) as revenue by l_orderkey, o_orderdate, o_shippriority | sort - revenue, o_orderdate| head 10")

    spark.sql("source = myglue_test.tpch_csv.orders| where o_orderdate >= date('1993-07-01')  and o_orderdate < date_add(date('1993-07-01'), interval 3 month)  and exists [    source = myglue_test.tpch_csv.lineitem    | where l_orderkey = o_orderkey and l_commitdate < l_receiptdate  ]| stats count() as order_count by o_orderpriority| sort o_orderpriority")

    spark.sql("source = myglue_test.tpch_csv.customer| join ON c_custkey = o_custkey myglue_test.tpch_csv.orders| join ON l_orderkey = o_orderkey myglue_test.tpch_csv.lineitem| join ON l_suppkey = s_suppkey AND c_nationkey = s_nationkey myglue_test.tpch_csv.supplier| join ON s_nationkey = n_nationkey myglue_test.tpch_csv.nation| join ON n_regionkey = r_regionkey myglue_test.tpch_csv.region| where r_name = 'ASIA' AND o_orderdate >= date('1994-01-01') AND o_orderdate < date_add(date('1994-01-01'), interval 1 year)| stats sum(l_extendedprice * (1 - l_discount)) as revenue by n_name| sort - revenue")

    spark.sql("source = myglue_test.tpch_csv.lineitem| where l_shipdate >= date('1994-01-01')  and l_shipdate < adddate(date('1994-01-01'), 365)  and l_discount between .06 - 0.01 and .06 + 0.01  and l_quantity < 24| stats sum(l_extendedprice * l_discount) as revenue")

    spark.sql("source = [    source = myglue_test.tpch_csv.supplier    | join ON s_suppkey = l_suppkey myglue_test.tpch_csv.lineitem    | join ON o_orderkey = l_orderkey myglue_test.tpch_csv.orders    | join ON c_custkey = o_custkey myglue_test.tpch_csv.customer    | join ON s_nationkey = n1.n_nationkey myglue_test.tpch_csv.nation as n1    | join ON c_nationkey = n2.n_nationkey myglue_test.tpch_csv.nation as n2    | where l_shipdate between date('1995-01-01') and date('1996-12-31')        and n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY' or n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE'    | eval supp_nation = n1.n_name, cust_nation = n2.n_name, l_year = year(l_shipdate), volume = l_extendedprice * (1 - l_discount)    | fields supp_nation, cust_nation, l_year, volume  ] as shipping| stats sum(volume) as revenue by supp_nation, cust_nation, l_year| sort supp_nation, cust_nation, l_year")

    spark.sql("source = [    source = myglue_test.tpch_csv.part    | join ON p_partkey = l_partkey myglue_test.tpch_csv.lineitem    | join ON s_suppkey = l_suppkey myglue_test.tpch_csv.supplier    | join ON l_orderkey = o_orderkey myglue_test.tpch_csv.orders    | join ON o_custkey = c_custkey myglue_test.tpch_csv.customer    | join ON c_nationkey = n1.n_nationkey myglue_test.tpch_csv.nation as n1    | join ON s_nationkey = n2.n_nationkey myglue_test.tpch_csv.nation as n2    | join ON n1.n_regionkey = r_regionkey myglue_test.tpch_csv.region    | where r_name = 'AMERICA' AND p_type = 'ECONOMY ANODIZED STEEL'      and o_orderdate between date('1995-01-01') and date('1996-12-31')    | eval o_year = year(o_orderdate)    | eval volume = l_extendedprice * (1 - l_discount)    | eval nation = n2.n_name    | fields o_year, volume, nation  ] as all_nations| stats sum(case(nation = 'BRAZIL', volume else 0)) as sum_case, sum(volume) as sum_volume by o_year| eval mkt_share = sum_case / sum_volume| fields mkt_share, o_year| sort o_year")

    spark.sql("source = [    source = myglue_test.tpch_csv.part    | join ON p_partkey = l_partkey myglue_test.tpch_csv.lineitem    | join ON s_suppkey = l_suppkey myglue_test.tpch_csv.supplier    | join ON ps_partkey = l_partkey and ps_suppkey = l_suppkey myglue_test.tpch_csv.partsupp    | join ON o_orderkey = l_orderkey myglue_test.tpch_csv.orders    | join ON s_nationkey = n_nationkey myglue_test.tpch_csv.nation    | where like(p_name, '%green%')    | eval nation = n_name    | eval o_year = year(o_orderdate)    | eval amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity    | fields nation, o_year, amount  ] as profit| stats sum(amount) as sum_profit by nation, o_year| sort nation, - o_year")

    spark.sql("source = myglue_test.tpch_csv.customer| join ON c_custkey = o_custkey myglue_test.tpch_csv.orders| join ON l_orderkey = o_orderkey myglue_test.tpch_csv.lineitem| join ON c_nationkey = n_nationkey myglue_test.tpch_csv.nation| where o_orderdate >= date('1993-10-01')  AND o_orderdate < date_add(date('1993-10-01'), interval 3 month)  AND l_returnflag = 'R'| stats sum(l_extendedprice * (1 - l_discount)) as revenue by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment| sort - revenue| head 20")

    spark.sql("source = myglue_test.tpch_csv.partsupp| join ON ps_suppkey = s_suppkey myglue_test.tpch_csv.supplier| join ON s_nationkey = n_nationkey myglue_test.tpch_csv.nation| where n_name = 'GERMANY'| stats sum(ps_supplycost * ps_availqty) as value by ps_partkey| where value > [    source = myglue_test.tpch_csv.partsupp    | join ON ps_suppkey = s_suppkey myglue_test.tpch_csv.supplier    | join ON s_nationkey = n_nationkey myglue_test.tpch_csv.nation    | where n_name = 'GERMANY'    | stats sum(ps_supplycost * ps_availqty) as check    | eval threshold = check * 0.0001000000    | fields threshold  ]| sort - value")

    spark.sql("source = myglue_test.tpch_csv.orders| join ON o_orderkey = l_orderkey myglue_test.tpch_csv.lineitem| where l_commitdate < l_receiptdate    and l_shipdate < l_commitdate    and l_shipmode in ('MAIL', 'SHIP')    and l_receiptdate >= date('1994-01-01')    and l_receiptdate < date_add(date('1994-01-01'), interval 1 year)| stats sum(case(o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH', 1 else 0)) as high_line_count,        sum(case(o_orderpriority != '1-URGENT' and o_orderpriority != '2-HIGH', 1 else 0)) as low_line_countby        by l_shipmode| sort l_shipmode")

    spark.sql("source = [    source = myglue_test.tpch_csv.customer    | left outer join ON c_custkey = o_custkey AND not like(o_comment, '%special%requests%')      myglue_test.tpch_csv.orders    | stats count(o_orderkey) as c_count by c_custkey  ] as c_orders| stats count() as custdist by c_count| sort - custdist, - c_count")

    spark.sql("source = myglue_test.tpch_csv.lineitem| join ON l_partkey = p_partkey    AND l_shipdate >= date('1995-09-01')    AND l_shipdate < date_add(date('1995-09-01'), interval 1 month)  myglue_test.tpch_csv.part| stats sum(case(like(p_type, 'PROMO%'), l_extendedprice * (1 - l_discount) else 0)) as sum1,        sum(l_extendedprice * (1 - l_discount)) as sum2| eval promo_revenue = 100.00 * sum1 / sum2 // Stats and Eval commands can combine when issues/819 resolved| fields promo_revenue")

    spark.sql("source = myglue_test.tpch_csv.supplier| join right = revenue0 ON s_suppkey = supplier_no [    source = myglue_test.tpch_csv.lineitem    | where l_shipdate >= date('1996-01-01') AND l_shipdate < date_add(date('1996-01-01'), interval 3 month)    | eval supplier_no = l_suppkey    | stats sum(l_extendedprice * (1 - l_discount)) as total_revenue by supplier_no  ]| where total_revenue = [    source = [        source = myglue_test.tpch_csv.lineitem        | where l_shipdate >= date('1996-01-01') AND l_shipdate < date_add(date('1996-01-01'), interval 3 month)        | eval supplier_no = l_suppkey        | stats sum(l_extendedprice * (1 - l_discount)) as total_revenue by supplier_no      ]    | stats max(total_revenue)  ]| sort s_suppkey| fields s_suppkey, s_name, s_address, s_phone, total_revenue")

    spark.sql("source = myglue_test.tpch_csv.partsupp| join ON p_partkey = ps_partkey myglue_test.tpch_csv.part| where p_brand != 'Brand#45'    and not like(p_type, 'MEDIUM POLISHED%')    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)    and ps_suppkey not in [          source = myglue_test.tpch_csv.supplier          | where like(s_comment, '%Customer%Complaints%')          | fields s_suppkey        ]| stats distinct_count(ps_suppkey) as supplier_cnt by p_brand, p_type, p_size| sort - supplier_cnt, p_brand, p_type, p_size")

    spark.sql("source = myglue_test.tpch_csv.lineitem| join ON p_partkey = l_partkey myglue_test.tpch_csv.part| where p_brand = 'Brand#23'    and p_container = 'MED BOX'    and l_quantity < [          source = myglue_test.tpch_csv.lineitem          | where l_partkey = p_partkey          | stats avg(l_quantity) as avg          | eval `0.2 * avg` = 0.2 * avg          | fields `0.2 * avg`        ]| stats sum(l_extendedprice) as sum| eval avg_yearly = sum / 7.0| fields avg_yearly")

    spark.sql("source = myglue_test.tpch_csv.customer| join ON c_custkey = o_custkey myglue_test.tpch_csv.orders| join ON o_orderkey = l_orderkey myglue_test.tpch_csv.lineitem| where o_orderkey in [    source = myglue_test.tpch_csv.lineitem    | stats sum(l_quantity) as sum by l_orderkey    | where sum > 300    | fields l_orderkey  ]| stats sum(l_quantity) by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice| sort - o_totalprice, o_orderdate| head 100")

    spark.sql("source = myglue_test.tpch_csv.lineitem| join ON p_partkey = l_partkey     and p_brand = 'Brand#12'     and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')     and l_quantity >= 1 and l_quantity <= 1 + 10     and p_size between 1 and 5     and l_shipmode in ('AIR', 'AIR REG')     and l_shipinstruct = 'DELIVER IN PERSON'     OR p_partkey = l_partkey     and p_brand = 'Brand#23'     and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')     and l_quantity >= 10 and l_quantity <= 10 + 10     and p_size between 1 and 10     and l_shipmode in ('AIR', 'AIR REG')     and l_shipinstruct = 'DELIVER IN PERSON'     OR p_partkey = l_partkey     and p_brand = 'Brand#34'     and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')     and l_quantity >= 20 and l_quantity <= 20 + 10     and p_size between 1 and 15     and l_shipmode in ('AIR', 'AIR REG')     and l_shipinstruct = 'DELIVER IN PERSON'  myglue_test.tpch_csv.part")

    spark.sql("source = myglue_test.tpch_csv.supplier| join ON s_nationkey = n_nationkey myglue_test.tpch_csv.nation| where n_name = 'CANADA'  and s_suppkey in [    source = myglue_test.tpch_csv.partsupp    | where ps_partkey in [        source = myglue_test.tpch_csv.part        | where like(p_name, 'forest%')        | fields p_partkey      ]      and ps_availqty > [        source = myglue_test.tpch_csv.lineitem        | where l_partkey = ps_partkey          and l_suppkey = ps_suppkey          and l_shipdate >= date('1994-01-01')          and l_shipdate < date_add(date('1994-01-01'), interval 1 year)        | stats sum(l_quantity) as sum_l_quantity        | eval half_sum_l_quantity = 0.5 * sum_l_quantity        | fields half_sum_l_quantity      ]    | fields ps_suppkey  ]")

    spark.sql("source = myglue_test.tpch_csv.supplier| join ON s_suppkey = l1.l_suppkey myglue_test.tpch_csv.lineitem as l1| join ON o_orderkey = l1.l_orderkey myglue_test.tpch_csv.orders| join ON s_nationkey = n_nationkey myglue_test.tpch_csv.nation| where o_orderstatus = 'F'  and l1.l_receiptdate > l1.l_commitdate  and exists [    source = myglue_test.tpch_csv.lineitem as l2    | where l2.l_orderkey = l1.l_orderkey      and l2.l_suppkey != l1.l_suppkey  ]  and not exists [    source = myglue_test.tpch_csv.lineitem as l3    | where l3.l_orderkey = l1.l_orderkey      and l3.l_suppkey != l1.l_suppkey      and l3.l_receiptdate > l3.l_commitdate  ]  and n_name = 'SAUDI ARABIA'| stats count() as numwait by s_name| sort - numwait, s_name| head 100")

    spark.sql("source = [  source = myglue_test.tpch_csv.customer    | where substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')      and c_acctbal > [          source = myglue_test.tpch_csv.customer          | where c_acctbal > 0.00            and substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')          | stats avg(c_acctbal)        ]      and not exists [          source = myglue_test.tpch_csv.orders          | where o_custkey = c_custkey        ]    | eval cntrycode = substring(c_phone, 1, 2)    | fields cntrycode, c_acctbal  ] as custsale| stats count() as numcust, sum(c_acctbal) as totacctbal by cntrycode| sort cntrycode")

}
