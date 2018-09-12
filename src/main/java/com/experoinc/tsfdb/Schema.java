package com.experoinc.tsfdb;

import com.experoinc.tsfdb.schema.AddFieldCommand;
import com.experoinc.tsfdb.schema.AddMeasurementCommand;
import com.experoinc.tsfdb.schema.SchemaCommand;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@NoArgsConstructor
public class Schema {

    private Map<Long, String> measurementNameMap = new ConcurrentHashMap<>();
    private Map<Long, String> fieldNameMap = new ConcurrentHashMap<>();
    private Map<Long, Set<Long>> measurementFields = new ConcurrentHashMap<>();

    public synchronized void updateSchema(final SchemaCommand message) {
        if (message instanceof AddMeasurementCommand) {
            final AddMeasurementCommand addMeasurement = (AddMeasurementCommand) message;
            measurementNameMap.put(addMeasurement.getId(), addMeasurement.getName());
        } else if (message instanceof AddFieldCommand) {
            final AddFieldCommand addField = (AddFieldCommand) message;
            fieldNameMap.put(addField.getId(), addField.getName());
            if (!measurementFields.containsKey(addField.getMeasurementId())) {
                final Set<Long> fields = new HashSet<>();
                fields.add(addField.getId());
                measurementFields.put(addField.getMeasurementId(), fields);
            } else {
                measurementFields.get(addField.getMeasurementId()).add(addField.getId());
            }
        }
    }

    public Set<Long> getFields(final Long measurementId) {
        return measurementFields.get(measurementId);
    }
}
