import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1772102229711 = glueContext.create_dynamic_frame.from_catalog(database="amazon_sales_db", table_name="wonji_amazon_sales_virginia", transformation_ctx="AWSGlueDataCatalog_node1772102229711")

# Script generated for node wonji-schema-0226
wonjischema0226_node1772102620295 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1772102229711, mappings=[("order_id", "long", "order_id", "long"), ("order_date", "string", "order_date", "string"), ("product_id", "long", "product_id", "long"), ("product_category", "string", "product_category", "string"), ("customer_region", "string", "customer_region", "string"), ("rating", "double", "rating", "double"), ("discounted_price", "double", "discounted_price", "double"), ("total_revenue", "double", "total_revenue", "double")], transformation_ctx="wonjischema0226_node1772102620295")

# Script generated for node wonji-evaluate-data-quality-0226
wonjievaluatedataquality0226_node1772102898775_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        Completeness "product_id" = 1.0,
        
        ColumnValues "rating" between 0.9 and 5.1,
        
        ColumnValues "discounted_price" > 0,
        
        ColumnCount = 8
    ]
"""

wonjievaluatedataquality0226_node1772102898775 = EvaluateDataQuality().process_rows(frame=wonjischema0226_node1772102620295, ruleset=wonjievaluatedataquality0226_node1772102898775_ruleset, publishing_options={"dataQualityEvaluationContext": "wonjievaluatedataquality0226_node1772102898775", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1772103894775 = SelectFromCollection.apply(dfc=wonjievaluatedataquality0226_node1772102898775, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1772103894775")

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1772103902370 = SelectFromCollection.apply(dfc=wonjievaluatedataquality0226_node1772102898775, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1772103902370")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=wonjischema0226_node1772102620295, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772102227308", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1772102631584 = glueContext.write_dynamic_frame.from_options(frame=wonjischema0226_node1772102620295, connection_type="s3", format="glueparquet", connection_options={"path": "s3://wonji-amazon-sales-virginia/csv-to-parquet/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1772102631584")

job.commit()