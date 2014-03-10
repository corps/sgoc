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
public class IndexEntries extends org.jooq.impl.TableImpl<io.corps.sgoc.schema.tables.records.IndexEntriesRecord> {

	private static final long serialVersionUID = 1014614402;

	/**
	 * The singleton instance of <code>sgoc_test.index_entries</code>
	 */
	public static final io.corps.sgoc.schema.tables.IndexEntries INDEX_ENTRIES = new io.corps.sgoc.schema.tables.IndexEntries();

	/**
	 * The class holding records for this type
	 */
	@Override
	public java.lang.Class<io.corps.sgoc.schema.tables.records.IndexEntriesRecord> getRecordType() {
		return io.corps.sgoc.schema.tables.records.IndexEntriesRecord.class;
	}

	/**
	 * The column <code>sgoc_test.index_entries.root_key</code>.
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.records.IndexEntriesRecord, java.lang.String> ROOT_KEY = createField("root_key", org.jooq.impl.SQLDataType.VARCHAR.length(72).nullable(false), this);

	/**
	 * The column <code>sgoc_test.index_entries.uuid</code>.
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.records.IndexEntriesRecord, java.lang.String> UUID = createField("uuid", org.jooq.impl.SQLDataType.VARCHAR.length(72).nullable(false), this);

	/**
	 * The column <code>sgoc_test.index_entries.index_key</code>. binary of the proto
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.records.IndexEntriesRecord, byte[]> INDEX_KEY = createField("index_key", org.jooq.impl.SQLDataType.VARBINARY.length(767).nullable(false), this);

	/**
	 * The column <code>sgoc_test.index_entries.index_value</code>. binary of the proto
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.records.IndexEntriesRecord, byte[]> INDEX_VALUE = createField("index_value", org.jooq.impl.SQLDataType.VARBINARY.length(767).nullable(false), this);

	/**
	 * Create a <code>sgoc_test.index_entries</code> table reference
	 */
	public IndexEntries() {
		super("index_entries", io.corps.sgoc.schema.SgocTest.SGOC_TEST);
	}

	/**
	 * Create an aliased <code>sgoc_test.index_entries</code> table reference
	 */
	public IndexEntries(java.lang.String alias) {
		super(alias, io.corps.sgoc.schema.SgocTest.SGOC_TEST, io.corps.sgoc.schema.tables.IndexEntries.INDEX_ENTRIES);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public org.jooq.UniqueKey<io.corps.sgoc.schema.tables.records.IndexEntriesRecord> getPrimaryKey() {
		return io.corps.sgoc.schema.Keys.KEY_INDEX_ENTRIES_PRIMARY;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public java.util.List<org.jooq.UniqueKey<io.corps.sgoc.schema.tables.records.IndexEntriesRecord>> getKeys() {
		return java.util.Arrays.<org.jooq.UniqueKey<io.corps.sgoc.schema.tables.records.IndexEntriesRecord>>asList(io.corps.sgoc.schema.Keys.KEY_INDEX_ENTRIES_PRIMARY);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public java.util.List<org.jooq.ForeignKey<io.corps.sgoc.schema.tables.records.IndexEntriesRecord, ?>> getReferences() {
		return java.util.Arrays.<org.jooq.ForeignKey<io.corps.sgoc.schema.tables.records.IndexEntriesRecord, ?>>asList(io.corps.sgoc.schema.Keys.INDEX_ENTRIES_IBFK_1);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public io.corps.sgoc.schema.tables.IndexEntries as(java.lang.String alias) {
		return new io.corps.sgoc.schema.tables.IndexEntries(alias);
	}
}
