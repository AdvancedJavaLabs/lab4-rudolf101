package edu.itmo

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

class SalesBook {

    class LineProcessor : Mapper<Any, Text, Text, DataRecord>() {

        private val dataRecord = DataRecord()

        override fun map(key: Any, value: Text, context: Context) {
            val record = value.toString().split(",").map { it.trim() }

            if (record.size < 5) return

            val category = record[2]
            val price = record[3].toDoubleOrNull() ?: return
            val quantity = record[4].toLongOrNull() ?: return

            dataRecord.apply {
                this.category.set(category)
                this.totalQuantity.set(quantity)
                this.totalRevenue.set(price)
            }

            context.write(Text(category), dataRecord)
        }
    }

    class DataAggregator : Reducer<Text, DataRecord, Text, DataRecord>() {

        private val aggregatedKey = DataRecord()

        override fun reduce(categoryKey: Text, records: Iterable<DataRecord>, context: Context) {
            var totalQuantity = 0L
            var totalRevenue = 0.0

            for (record in records) {
                totalQuantity += record.totalQuantity.get()
                totalRevenue += record.totalRevenue.get()
            }

            aggregatedKey.apply {
                this.category.set(categoryKey)
                this.totalQuantity.set(totalQuantity)
                this.totalRevenue.set(totalRevenue)
            }

            context.write(categoryKey, aggregatedKey)
        }
    }
}
