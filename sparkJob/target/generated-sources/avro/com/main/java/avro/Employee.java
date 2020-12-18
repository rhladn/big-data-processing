/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.main.java.avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Employee extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Employee\",\"namespace\":\"com.main.java.avro\",\"fields\":[{\"name\":\"empid\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"empdame\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"email\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"phone\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"empdata\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.String empid;
  @Deprecated public java.lang.String empdame;
  @Deprecated public java.lang.String email;
  @Deprecated public java.lang.String phone;
  @Deprecated public java.lang.String empdata;

  /**
   * Default constructor.
   */
  public Employee() {}

  /**
   * All-args constructor.
   */
  public Employee(java.lang.String empid, java.lang.String empdame, java.lang.String email, java.lang.String phone, java.lang.String empdata) {
    this.empid = empid;
    this.empdame = empdame;
    this.email = email;
    this.phone = phone;
    this.empdata = empdata;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return empid;
    case 1: return empdame;
    case 2: return email;
    case 3: return phone;
    case 4: return empdata;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: empid = (java.lang.String)value$; break;
    case 1: empdame = (java.lang.String)value$; break;
    case 2: email = (java.lang.String)value$; break;
    case 3: phone = (java.lang.String)value$; break;
    case 4: empdata = (java.lang.String)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'empid' field.
   */
  public java.lang.String getEmpid() {
    return empid;
  }

  /**
   * Sets the value of the 'empid' field.
   * @param value the value to set.
   */
  public void setEmpid(java.lang.String value) {
    this.empid = value;
  }

  /**
   * Gets the value of the 'empdame' field.
   */
  public java.lang.String getEmpdame() {
    return empdame;
  }

  /**
   * Sets the value of the 'empdame' field.
   * @param value the value to set.
   */
  public void setEmpdame(java.lang.String value) {
    this.empdame = value;
  }

  /**
   * Gets the value of the 'email' field.
   */
  public java.lang.String getEmail() {
    return email;
  }

  /**
   * Sets the value of the 'email' field.
   * @param value the value to set.
   */
  public void setEmail(java.lang.String value) {
    this.email = value;
  }

  /**
   * Gets the value of the 'phone' field.
   */
  public java.lang.String getPhone() {
    return phone;
  }

  /**
   * Sets the value of the 'phone' field.
   * @param value the value to set.
   */
  public void setPhone(java.lang.String value) {
    this.phone = value;
  }

  /**
   * Gets the value of the 'empdata' field.
   */
  public java.lang.String getEmpdata() {
    return empdata;
  }

  /**
   * Sets the value of the 'empdata' field.
   * @param value the value to set.
   */
  public void setEmpdata(java.lang.String value) {
    this.empdata = value;
  }

  /** Creates a new Employee RecordBuilder */
  public static com.main.java.avro.Employee.Builder newBuilder() {
    return new com.main.java.avro.Employee.Builder();
  }
  
  /** Creates a new Employee RecordBuilder by copying an existing Builder */
  public static com.main.java.avro.Employee.Builder newBuilder(com.main.java.avro.Employee.Builder other) {
    return new com.main.java.avro.Employee.Builder(other);
  }
  
  /** Creates a new Employee RecordBuilder by copying an existing Employee instance */
  public static com.main.java.avro.Employee.Builder newBuilder(com.main.java.avro.Employee other) {
    return new com.main.java.avro.Employee.Builder(other);
  }
  
  /**
   * RecordBuilder for Employee instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Employee>
    implements org.apache.avro.data.RecordBuilder<Employee> {

    private java.lang.String empid;
    private java.lang.String empdame;
    private java.lang.String email;
    private java.lang.String phone;
    private java.lang.String empdata;

    /** Creates a new Builder */
    private Builder() {
      super(com.main.java.avro.Employee.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.main.java.avro.Employee.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing Employee instance */
    private Builder(com.main.java.avro.Employee other) {
            super(com.main.java.avro.Employee.SCHEMA$);
      if (isValidValue(fields()[0], other.empid)) {
        this.empid = data().deepCopy(fields()[0].schema(), other.empid);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.empdame)) {
        this.empdame = data().deepCopy(fields()[1].schema(), other.empdame);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.email)) {
        this.email = data().deepCopy(fields()[2].schema(), other.email);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.phone)) {
        this.phone = data().deepCopy(fields()[3].schema(), other.phone);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.empdata)) {
        this.empdata = data().deepCopy(fields()[4].schema(), other.empdata);
        fieldSetFlags()[4] = true;
      }
    }

    /** Gets the value of the 'empid' field */
    public java.lang.String getEmpid() {
      return empid;
    }
    
    /** Sets the value of the 'empid' field */
    public com.main.java.avro.Employee.Builder setEmpid(java.lang.String value) {
      validate(fields()[0], value);
      this.empid = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'empid' field has been set */
    public boolean hasEmpid() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'empid' field */
    public com.main.java.avro.Employee.Builder clearEmpid() {
      empid = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'empdame' field */
    public java.lang.String getEmpdame() {
      return empdame;
    }
    
    /** Sets the value of the 'empdame' field */
    public com.main.java.avro.Employee.Builder setEmpdame(java.lang.String value) {
      validate(fields()[1], value);
      this.empdame = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'empdame' field has been set */
    public boolean hasEmpdame() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'empdame' field */
    public com.main.java.avro.Employee.Builder clearEmpdame() {
      empdame = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'email' field */
    public java.lang.String getEmail() {
      return email;
    }
    
    /** Sets the value of the 'email' field */
    public com.main.java.avro.Employee.Builder setEmail(java.lang.String value) {
      validate(fields()[2], value);
      this.email = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'email' field has been set */
    public boolean hasEmail() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'email' field */
    public com.main.java.avro.Employee.Builder clearEmail() {
      email = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'phone' field */
    public java.lang.String getPhone() {
      return phone;
    }
    
    /** Sets the value of the 'phone' field */
    public com.main.java.avro.Employee.Builder setPhone(java.lang.String value) {
      validate(fields()[3], value);
      this.phone = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'phone' field has been set */
    public boolean hasPhone() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'phone' field */
    public com.main.java.avro.Employee.Builder clearPhone() {
      phone = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'empdata' field */
    public java.lang.String getEmpdata() {
      return empdata;
    }
    
    /** Sets the value of the 'empdata' field */
    public com.main.java.avro.Employee.Builder setEmpdata(java.lang.String value) {
      validate(fields()[4], value);
      this.empdata = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'empdata' field has been set */
    public boolean hasEmpdata() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'empdata' field */
    public com.main.java.avro.Employee.Builder clearEmpdata() {
      empdata = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    public Employee build() {
      try {
        Employee record = new Employee();
        record.empid = fieldSetFlags()[0] ? this.empid : (java.lang.String) defaultValue(fields()[0]);
        record.empdame = fieldSetFlags()[1] ? this.empdame : (java.lang.String) defaultValue(fields()[1]);
        record.email = fieldSetFlags()[2] ? this.email : (java.lang.String) defaultValue(fields()[2]);
        record.phone = fieldSetFlags()[3] ? this.phone : (java.lang.String) defaultValue(fields()[3]);
        record.empdata = fieldSetFlags()[4] ? this.empdata : (java.lang.String) defaultValue(fields()[4]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
