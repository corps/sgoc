/**
 * This class is generated by jOOQ
 */
package io.corps.sgoc.schema.tables;

/**
 * This class is generated by jOOQ.
 */
@javax.annotation.Generated(value    = { "http://www.jooq.org", "3.2.3" },
                            comments = "This class is generated by jOOQ")
@java.lang.SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Objects extends org.jooq.impl.TableImpl<io.corps.sgoc.schema.tables.records.ObjectsRecord> {

	private static final long serialVersionUID = 1043128815;

	/**
	 * The singleton instance of <code>sgoc_test.objects</code>
	 */
	public static final io.corps.sgoc.schema.tables.Objects OBJECTS = new io.corps.sgoc.schema.tables.Objects();

	/**
	 * The class holding records for this type
	 */
	@Override
	public java.lang.Class<io.corps.sgoc.schema.tables.records.ObjectsRecord> getRecordType() {
		return io.corps.sgoc.schema.tables.records.ObjectsRecord.class;
	}

	/**
	 * The column <code>sgoc_test.objects.root_key</code>.
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.records.ObjectsRecord, java.lang.String> ROOT_KEY = createField("root_key", org.jooq.impl.SQLDataType.VARCHAR.length(72).nullable(false), this);

	/**
	 * The column <code>sgoc_test.objects.uuid</code>.
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.records.ObjectsRecord, java.lang.String> UUID = createField("uuid", org.jooq.impl.SQLDataType.VARCHAR.length(72).nullable(false), this);

	/**
	 * The column <code>sgoc_test.objects.object</code>.
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.records.ObjectsRecord, byte[]> OBJECT = createField("object", org.jooq.impl.SQLDataType.BLOB.nullable(false), this);

	/**
	 * The column <code>sgoc_test.objects.deleted</code>. Only used for filtering on initial import.
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.records.ObjectsRecord, java.lang.Byte> DELETED = createField("deleted", org.jooq.impl.SQLDataType.TINYINT.nullable(false).defaulted(true), this);

	/**
	 * The column <code>sgoc_test.objects.timestamp</code>.
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.records.ObjectsRecord, java.lang.Long> TIMESTAMP = createField("timestamp", org.jooq.impl.SQLDataType.BIGINT.nullable(false), this);

	/**
	 * Create a <code>sgoc_test.objects</code> table reference
	 */
	public Objects() {
		super("objects", io.corps.sgoc.schema.SgocTest.SGOC_TEST);
	}

	/**
	 * Create an aliased <code>sgoc_test.objects</code> table reference
	 */
	public Objects(java.lang.String alias) {
		super(alias, io.corps.sgoc.schema.SgocTest.SGOC_TEST, io.corps.sgoc.schema.tables.Objects.OBJECTS);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public org.jooq.UniqueKey<io.corps.sgoc.schema.tables.records.ObjectsRecord> getPrimaryKey() {
		return io.corps.sgoc.schema.Keys.KEY_OBJECTS_PRIMARY;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public java.util.List<org.jooq.UniqueKey<io.corps.sgoc.schema.tables.records.ObjectsRecord>> getKeys() {
		return java.util.Arrays.<org.jooq.UniqueKey<io.corps.sgoc.schema.tables.records.ObjectsRecord>>asList(io.corps.sgoc.schema.Keys.KEY_OBJECTS_PRIMARY);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public io.corps.sgoc.schema.tables.Objects as(java.lang.String alias) {
		return new io.corps.sgoc.schema.tables.Objects(alias);
	}
}
