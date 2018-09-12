package com.experoinc.tsfdb.schema;

import lombok.Data;

/**
 * @author twilmes
 */
@Data
public class AddMeasurementCommand extends SchemaCommand {

    private final Long id;
    private final String name;
}
