/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.main.java.avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class CohortInfo extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"CohortInfo\",\"namespace\":\"com.main.java.avro\",\"fields\":[{\"name\":\"cohortId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.String cohortId;

  /**
   * Default constructor.
   */
  public CohortInfo() {}

  /**
   * All-args constructor.
   */
  public CohortInfo(java.lang.String cohortId) {
    this.cohortId = cohortId;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return cohortId;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: cohortId = (java.lang.String)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'cohortId' field.
   */
  public java.lang.String getCohortId() {
    return cohortId;
  }

  /**
   * Sets the value of the 'cohortId' field.
   * @param value the value to set.
   */
  public void setCohortId(java.lang.String value) {
    this.cohortId = value;
  }

  /** Creates a new CohortInfo RecordBuilder */
  public static com.main.java.avro.CohortInfo.Builder newBuilder() {
    return new com.main.java.avro.CohortInfo.Builder();
  }
  
  /** Creates a new CohortInfo RecordBuilder by copying an existing Builder */
  public static com.main.java.avro.CohortInfo.Builder newBuilder(com.main.java.avro.CohortInfo.Builder other) {
    return new com.main.java.avro.CohortInfo.Builder(other);
  }
  
  /** Creates a new CohortInfo RecordBuilder by copying an existing CohortInfo instance */
  public static com.main.java.avro.CohortInfo.Builder newBuilder(com.main.java.avro.CohortInfo other) {
    return new com.main.java.avro.CohortInfo.Builder(other);
  }
  
  /**
   * RecordBuilder for CohortInfo instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<CohortInfo>
    implements org.apache.avro.data.RecordBuilder<CohortInfo> {

    private java.lang.String cohortId;

    /** Creates a new Builder */
    private Builder() {
      super(com.main.java.avro.CohortInfo.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.main.java.avro.CohortInfo.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing CohortInfo instance */
    private Builder(com.main.java.avro.CohortInfo other) {
            super(com.main.java.avro.CohortInfo.SCHEMA$);
      if (isValidValue(fields()[0], other.cohortId)) {
        this.cohortId = data().deepCopy(fields()[0].schema(), other.cohortId);
        fieldSetFlags()[0] = true;
      }
    }

    /** Gets the value of the 'cohortId' field */
    public java.lang.String getCohortId() {
      return cohortId;
    }
    
    /** Sets the value of the 'cohortId' field */
    public com.main.java.avro.CohortInfo.Builder setCohortId(java.lang.String value) {
      validate(fields()[0], value);
      this.cohortId = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'cohortId' field has been set */
    public boolean hasCohortId() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'cohortId' field */
    public com.main.java.avro.CohortInfo.Builder clearCohortId() {
      cohortId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    public CohortInfo build() {
      try {
        CohortInfo record = new CohortInfo();
        record.cohortId = fieldSetFlags()[0] ? this.cohortId : (java.lang.String) defaultValue(fields()[0]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}