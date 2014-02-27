/**
 * This class is generated by jOOQ
 */
package io.corps.sgoc.schema.tables.tables;

/**
 * This class is generated by jOOQ.
 */
@javax.annotation.Generated(value    = { "http://www.jooq.org", "3.2.3" },
                            comments = "This class is generated by jOOQ")
@java.lang.SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class LogEntries extends org.jooq.impl.TableImpl<io.corps.sgoc.schema.tables.tables.records.LogEntriesRecord> {

	private static final long serialVersionUID = 159553262;

	/**
	 * The singleton instance of <code>sgoc_test.log_entries</code>
	 */
	public static final io.corps.sgoc.schema.tables.tables.LogEntries LOG_ENTRIES = new io.corps.sgoc.schema.tables.tables.LogEntries();

	/**
	 * The class holding records for this type
	 */
	@Override
	public java.lang.Class<io.corps.sgoc.schema.tables.tables.records.LogEntriesRecord> getRecordType() {
		return io.corps.sgoc.schema.tables.tables.records.LogEntriesRecord.class;
	}

	/**
	 * The column <code>sgoc_test.log_entries.root_key</code>.
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.tables.records.LogEntriesRecord, java.lang.String> ROOT_KEY = createField("root_key", org.jooq.impl.SQLDataType.VARCHAR.length(32).nullable(false), this);

	/**
	 * The column <code>sgoc_test.log_entries.sequence</code>.
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.tables.records.LogEntriesRecord, java.lang.Long> SEQUENCE = createField("sequence", org.jooq.impl.SQLDataType.BIGINT.nullable(false), this);

	/**
	 * The column <code>sgoc_test.log_entries.timestamp</code>.
	 */
	public final org.jooq.TableField<io.corps.sgoc.schema.tables.tables.records.LogEntriesRecord, java.lang.Long> TIMESTAMP = createField("timestamp", org.jooq.impl.SQLDataType.BIGINT.nullable(false), this);

	/**
	 * Create a <code>sgoc_test.log_entries</code> table reference
	 */
	public LogEntries() {
		super("log_entries", io.corps.sgoc.schema.tables.SgocTest.SGOC_TEST);
	}

	/**
	 * Create an aliased <code>sgoc_test.log_entries</code> table reference
	 */
	public LogEntries(java.lang.String alias) {
		super(alias, io.corps.sgoc.schema.tables.SgocTest.SGOC_TEST, io.corps.sgoc.schema.tables.tables.LogEntries.LOG_ENTRIES);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public org.jooq.UniqueKey<io.corps.sgoc.schema.tables.tables.records.LogEntriesRecord> getPrimaryKey() {
		return io.corps.sgoc.schema.tables.Keys.KEY_LOG_ENTRIES_PRIMARY;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public java.util.List<org.jooq.UniqueKey<io.corps.sgoc.schema.tables.tables.records.LogEntriesRecord>> getKeys() {
		return java.util.Arrays.<org.jooq.UniqueKey<io.corps.sgoc.schema.tables.tables.records.LogEntriesRecord>>asList(io.corps.sgoc.schema.tables.Keys.KEY_LOG_ENTRIES_PRIMARY, io.corps.sgoc.schema.tables.Keys.KEY_LOG_ENTRIES_IDX_UNIQUE_USER_TOKEN_TIMESTAMP);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public io.corps.sgoc.schema.tables.tables.LogEntries as(java.lang.String alias) {
		return new io.corps.sgoc.schema.tables.tables.LogEntries(alias);
	}
}
