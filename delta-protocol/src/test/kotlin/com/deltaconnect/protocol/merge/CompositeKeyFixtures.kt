package com.deltaconnect.protocol.merge

import com.deltaconnect.protocol.schema.AvroToDeltaConverter
import com.deltaconnect.protocol.schema.DeltaType
import com.deltaconnect.protocol.schema.StructField
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder

object CompositeKeyFixtures {
    val schema =
        DeltaType.StructType(
            listOf(
                StructField("tenant_id", DeltaType.StringType, nullable = false),
                StructField("id", DeltaType.IntegerType, nullable = false),
                StructField("name", DeltaType.StringType, nullable = true),
            ),
        )

    val avroSchema = AvroToDeltaConverter.toAvroSchema(schema)

    val keyNames = listOf("tenant_id", "id")

    fun record(
        tenantId: String,
        id: Int,
        name: String?,
    ): GenericRecord =
        GenericRecordBuilder(avroSchema)
            .set("tenant_id", tenantId)
            .set("id", id)
            .set("name", name)
            .build()
}
