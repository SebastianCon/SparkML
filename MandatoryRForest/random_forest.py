from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import functions, SparkSession
from pyspark.sql.functions import when
import time


if __name__ == "__main__":

    # Create the SparkSession.
    spark_session = SparkSession\
        .builder\
        .appName("Random Forest ")\
        .getOrCreate()

    # Define the initial time.
    initial_time = time.time()

    # Load the data with 'libsvm' format.
    data_frame = spark_session\
        .read\
        .format("libsvm")\
        .load("D:\\breastcancertask1.libsvm")

    # Change label
    data_frame = data_frame.withColumn("label", when(functions.col("label") == 2.0, 0.0).otherwise(1.0))

    # Split the dataset into training data and testing data. We use the 75% for training.
    (training_data, test_data) = data_frame.randomSplit([0.75, 0.25])

    # Let's see the quantity of training data, and another visualizations of it.
    print("training data: " + str(training_data.count()))
    training_data.printSchema()
    training_data.show()

    # The same for the testing data.
    print("test data: " + str(test_data.count()))
    test_data.printSchema()
    test_data.show()

    # Check the performance of the time before training, the training time (line 50), and the testing time (line 55).
    time_before_training = time.time()
    print("Time before training: " + str(time_before_training - initial_time))

    # Select the model
    random_forest = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20)

    # Train the model with the training data.
    model = random_forest.fit(training_data)
    training_time = time.time()
    print("Training time: " + str(training_time - time_before_training))

    # Make the predictions with testing data.
    prediction = model.transform(test_data)
    testing_time = time.time()
    print("Test time: " + str(testing_time - training_time))

    # Visualizations of predictions.
    prediction.printSchema()
    prediction.show()

    # Select (prediction, true label) and compute test error.
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(prediction)
    print("Test Error = %g " % accuracy)

    spark_session.stop()