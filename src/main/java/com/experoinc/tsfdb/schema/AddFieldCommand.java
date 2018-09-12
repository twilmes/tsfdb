package com.experoinc.tsfdb.schema;

import lombok.Data;

/**
 * @author twilmes
 */
@Data
public class AddFieldCommand extends SchemaCommand {

    private final Long id;
    private final String name;
    private final Long measurementId;
}
