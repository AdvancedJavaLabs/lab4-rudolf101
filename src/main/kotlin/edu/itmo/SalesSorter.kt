package edu.itmo

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import java.io.DataInput
import java.io.DataOutput
import java.util.Objects
import java.util.StringTokenizer

class RevenueSorter {
    class RecordHandler : Mapper<Any, Text, SalesRecord, Text>() {

        private val record = SalesRecord()
        private val empty = Text()

        override fun map(key: Any, value: Text, context: Context) {
            val tokenizer = StringTokenizer(value.toString(), "\n")

            while (tokenizer.hasMoreTokens()) {
                val category = tokenizer.nextToken(",")
                val revenue = tokenizer.nextToken(",").toDoubleOrNull() ?: return
                val quantity = tokenizer.nextToken().toLongOrNull() ?: return

                record.category.set(category)
                record.quantity.set(quantity)
                record.revenue.set(revenue)
                context.write(record, empty)
            }
        }
    }

    class RevenueReducer : Reducer<SalesRecord, Text, SalesRecord, Text>() {
        override fun reduce(key: SalesRecord, values: MutableIterable<Text>, context: Context) {
            for (value in values) {
                context.write(key, value)
            }
        }
    }
}

class SalesRecord(
    val category: Text,
    val revenue: DoubleWritable,
    val quantity: LongWritable
) : WritableComparable<SalesRecord> {

    constructor() : this(category = Text(), revenue = DoubleWritable(), quantity = LongWritable())

    override fun write(dest: DataOutput) {
        category.write(dest)
        revenue.write(dest)
        quantity.write(dest)
    }

    override fun readFields(src: DataInput) {
        category.readFields(src)
        revenue.readFields(src)
        quantity.readFields(src)
    }

    override fun compareTo(other: SalesRecord): Int {
        return other.revenue.compareTo(revenue)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SalesRecord) return false
        return category == other.category &&
                revenue == other.revenue &&
                quantity == other.quantity
    }

    override fun hashCode(): Int {
        return Objects.hash(category, revenue, quantity)
    }

    override fun toString(): String {
        return "$category\t${revenue.get()}\t$quantity"
    }
}
