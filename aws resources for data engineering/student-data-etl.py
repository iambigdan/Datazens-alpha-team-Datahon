import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Student Parent data
StudentParentdata_node1727889064132 = glueContext.create_dynamic_frame.from_catalog(database="student_data_dt", table_name="student_parent_csv", transformation_ctx="StudentParentdata_node1727889064132")

# Script generated for node Student Data
StudentData_node1727889013483 = glueContext.create_dynamic_frame.from_catalog(database="student_data_dt", table_name="student_data_csv", transformation_ctx="StudentData_node1727889013483")

# Script generated for node Change Schema for Parent data
ChangeSchemaforParentdata_node1727889129898 = ApplyMapping.apply(frame=StudentParentdata_node1727889064132, mappings=[("col0", "string", "Student_ID", "string"), ("col1", "string", "Father_Occupation", "string"), ("col2", "string", "Mother_Occupation", "string"), ("col3", "string", "Parent_Income_Level", "string")], transformation_ctx="ChangeSchemaforParentdata_node1727889129898")

# Script generated for node Join
StudentData_node1727889013483DF = StudentData_node1727889013483.toDF()
ChangeSchemaforParentdata_node1727889129898DF = ChangeSchemaforParentdata_node1727889129898.toDF()
Join_node1727889359104 = DynamicFrame.fromDF(StudentData_node1727889013483DF.join(ChangeSchemaforParentdata_node1727889129898DF, (StudentData_node1727889013483DF['student_id'] == ChangeSchemaforParentdata_node1727889129898DF['Student_ID']), "left"), glueContext, "Join_node1727889359104")

# Script generated for node Change Schema
ChangeSchema_node1727889810147 = ApplyMapping.apply(frame=Join_node1727889359104, mappings=[("student_id", "string", "student_id", "string"), ("first_name", "string", "first_name", "string"), ("last name", "string", "last name", "string"), ("age", "long", "age", "bigint"), ("gender", "string", "gender", "string"), ("library_weekly_hours", "long", "library_weekly_hours", "bigint"), ("class_weekly_attendance_hours", "long", "class_weekly_attendance_hours", "bigint"), ("class_weekly_attendance_percentage", "double", "class_weekly_attendance_percentage", "decimal"), ("extra-curricular_weekly_hours", "long", "extra-curricular_weekly_hours", "bigint"), ("previous_exam_score", "long", "previous_exam_score", "bigint"), ("Father_Occupation", "string", "Father_Occupation", "string"), ("Mother_Occupation", "string", "Mother_Occupation", "string"), ("Parent_Income_Level", "string", "Parent_Income_Level", "string")], transformation_ctx="ChangeSchema_node1727889810147")

# Script generated for node Amazon S3
AmazonS3_node1727889985778 = glueContext.getSink(path="s3://student-final-data", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1727889985778")
AmazonS3_node1727889985778.setCatalogInfo(catalogDatabase="student_final_data",catalogTableName="Students_final")
AmazonS3_node1727889985778.setFormat("glueparquet", compression="snappy")
AmazonS3_node1727889985778.writeFrame(ChangeSchema_node1727889810147)
job.commit()