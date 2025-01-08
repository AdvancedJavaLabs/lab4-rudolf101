package edu.itmo

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput
import java.util.Objects

class DataRecord(
    val category: Text,
    val totalRevenue: DoubleWritable,
    val totalQuantity: LongWritable
) : WritableComparable<DataRecord> {

    constructor() : this(category = Text(), totalRevenue = DoubleWritable(), totalQuantity = LongWritable())

    override fun write(out: DataOutput) {
        category.write(out)
        totalRevenue.write(out)
        totalQuantity.write(out)
    }

    override fun readFields(input: DataInput) {
        category.readFields(input)
        totalRevenue.readFields(input)
        totalQuantity.readFields(input)
    }

    override fun compareTo(other: DataRecord): Int {
        return other.totalRevenue.compareTo(this.totalRevenue)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DataRecord) return false
        return category == other.category &&
                totalRevenue == other.totalRevenue &&
                totalQuantity == other.totalQuantity
    }

    override fun hashCode(): Int {
        return Objects.hash(category, totalRevenue, totalQuantity)
    }

    override fun toString(): String {
        return "${totalRevenue.get()},${totalQuantity.get()}"
    }
}
