import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
            database="products_db", 
            table_name="products", 
            transformation_ctx="DataCatalogtable_node1"
            )

# Script generated for node Change Schema
ChangeSchema_node1695345018750 = ApplyMapping.apply(
        frame=DataCatalogtable_node1, 
        mappings=[
            ("product_id", "string", "product_id", "string"), 
            ("product_name", "string", "product_name", "string"), 
            ("category", "string", "category", "string"), 
            ("discounted_price", "string", "discounted_price", "string"), 
            ("actual_price", "string", "actual_price", "string"), 
            ("discount_percentage", "string", "discount_percentage", "string"), 
            ("rating_count", "string", "rating_count", "string"), 
            ("about_product", "string", "about_product", "string"), 
            ("user_id", "string", "user_id", "string"), 
            ("user_name", "string", "user_name", "string"), 
            ("review_id", "string", "review_id", "string"), 
            ("review_title", "string", "review_title", "string"), 
            ("review_content", "string", "review_content", "string"), 
            ("img_link", "string", "img_link", "string"), 
            ("product_link", "string", "product_link", "string")
            ], 
        transformation_ctx="ChangeSchema_node1695345018750"
        )

# Script generated for node Amazon Redshift
AmazonRedshift_node2 = glueContext.write_dynamic_frame.from_options(
            frame=ChangeSchema_node1695345018750, 
            connection_type="redshift", 
            connection_options={
                "postactions": "BEGIN; \
                                MERGE INTO public.products USING public.products_temp \
                                    ON products.product_id = products_temp.product_id \
                                    WHEN MATCHED THEN \
                                        UPDATE SET product_id = products_temp.product_id, \
                                                    product_name = products_temp.product_name, \
                                                    category = products_temp.category, \
                                                    discounted_price = products_temp.discounted_price, \
                                                    actual_price = products_temp.actual_price, \
                                                    discount_percentage = products_temp.discount_percentage, \
                                                    rating_count = products_temp.rating_count, \
                                                    about_product = products_temp.about_product, \
                                                    user_id = products_temp.user_id, \
                                                    user_name = products_temp.user_name, \
                                                    review_id = products_temp.review_id, \
                                                    review_title = products_temp.review_title, \
                                                    review_content = products_temp.review_content, \
                                                    img_link = products_temp.img_link, \
                                                    product_link = products_temp.product_link \
                                    WHEN NOT MATCHED THEN \
                                        INSERT VALUES ( \
                                                products_temp.product_id, \
                                                products_temp.product_name, \
                                                products_temp.category, \
                                                products_temp.discounted_price, \
                                                products_temp.actual_price, \
                                                products_temp.discount_percentage, \
                                                products_temp.rating_count, \
                                                products_temp.about_product, \
                                                products_temp.user_id, \
                                                products_temp.user_name, \
                                                products_temp.review_id, \
                                                products_temp.review_title, \
                                                products_temp.review_content, \
                                                products_temp.img_link, \
                                                products_temp.product_link); \
                                DROP TABLE public.products_temp; \
                                END;", 
                    "redshiftTmpDir": "s3://aws-glue-assets-575357430574-us-east-1/temporary/", 
                    "useConnectionProperties": "true", 
                    "dbtable": "public.products_temp", 
                    "connectionName": "conn_redshift", 
                    "preactions": "CREATE TABLE IF NOT EXISTS public.products ( \
                                        product_id VARCHAR, \
                                        product_name VARCHAR, \
                                        category VARCHAR, \
                                        discounted_price VARCHAR, \
                                        actual_price VARCHAR, \
                                        discount_percentage VARCHAR, \
                                        rating_count VARCHAR, \
                                        about_product VARCHAR, \
                                        user_id VARCHAR, \
                                        user_name VARCHAR, \
                                        review_id VARCHAR, \
                                        review_title VARCHAR, \
                                        review_content VARCHAR, \
                                        img_link VARCHAR, \
                                        product_link VARCHAR); \
                                    DROP TABLE IF EXISTS public.products_temp_0b5a0d; \
                                    CREATE TABLE public.products_temp_0b5a0d ( \
                                        product_id VARCHAR, \
                                        product_name VARCHAR, \
                                        category VARCHAR, \
                                        discounted_price VARCHAR, \
                                        actual_price VARCHAR, \
                                        discount_percentage VARCHAR, \
                                        rating_count VARCHAR, \
                                        about_product VARCHAR, \
                                        user_id VARCHAR, \
                                        user_name VARCHAR, \
                                        review_id VARCHAR, \
                                        review_title VARCHAR, \
                                        review_content VARCHAR, \
                                        img_link VARCHAR, \
                                        product_link VARCHAR);"}, 
            transformation_ctx="AmazonRedshift_node2")

job.commit()