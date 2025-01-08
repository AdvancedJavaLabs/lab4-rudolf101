package edu.itmo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.log4j.Logger
import kotlin.system.exitProcess

class Runner {

    companion object {
        private const val LOG_VERBOSE = false
        private val logger = Logger.getLogger(Runner::class.java)

        @JvmStatic
        fun main(args: Array<String>) {
            if (args.size < 2) {
                logger.info("Usage: Runner <input path> <output path>")
                exitProcess(1)
            }

            val intermediateOutput = Path("/tmp/reduced")
            val startReduction = System.currentTimeMillis()
            val reductionJob = setupReductionJob(Path(args[0]), intermediateOutput)
            if (!reductionJob.waitForCompletion(LOG_VERBOSE)) {
                logger.info("Reduction job failed!")
                exitProcess(1)
            }
            val reductionTime = System.currentTimeMillis() - startReduction
            logger.info("Reduction job completed in $reductionTime ms")

            val startSorting = System.currentTimeMillis()
            val sortingJob = setupSortingJob(intermediateOutput, Path(args[1]))
            if (!sortingJob.waitForCompletion(LOG_VERBOSE)) {
                logger.info("Sorting job failed!")
                exitProcess(1)
            }
            val sortingTime = System.currentTimeMillis() - startSorting
            logger.info("Sorting job completed in $sortingTime ms")

            val totalTime = reductionTime + sortingTime
            logger.info("Total execution time: $totalTime ms")
        }

        private fun setupReductionJob(inputPath: Path, outputPath: Path): Job {
            val configuration = Configuration().apply {
                set(TextOutputFormat.SEPARATOR, ",")
            }

            return Job.getInstance(configuration).apply {
                setJarByClass(Runner::class.java)
                mapperClass = SalesBook.LineProcessor::class.java

                mapOutputKeyClass = Text::class.java
                mapOutputValueClass = DataRecord::class.java

                reducerClass = SalesBook.DataAggregator::class.java

                outputKeyClass = Text::class.java
                outputValueClass = DataRecord::class.java

                FileInputFormat.addInputPath(this, inputPath)
                FileOutputFormat.setOutputPath(this, outputPath)

                outputPath.getFileSystem(configuration).delete(outputPath, true)
            }
        }

        private fun setupSortingJob(inputPath: Path, outputPath: Path): Job {
            val configuration = Configuration()

            return Job.getInstance(configuration).apply {
                setJarByClass(Runner::class.java)
                mapperClass = RevenueSorter.RecordHandler::class.java

                mapOutputKeyClass = SalesRecord::class.java
                mapOutputValueClass = Text::class.java

                reducerClass = RevenueSorter.RevenueReducer::class.java

                outputKeyClass = SalesRecord::class.java
                outputValueClass = Text::class.java

                FileInputFormat.addInputPath(this, inputPath)
                FileOutputFormat.setOutputPath(this, outputPath)

                outputPath.getFileSystem(configuration).delete(outputPath, true)
            }
        }
    }
}
