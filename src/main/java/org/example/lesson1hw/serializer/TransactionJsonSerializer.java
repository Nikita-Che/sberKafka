package org.example.lesson1hw.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.example.lesson1hw.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class TransactionJsonSerializer implements Serializer<Transaction> {

    private static final Logger log = LoggerFactory.getLogger(TransactionJsonSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private JsonSchema schema;

    public TransactionJsonSerializer() {
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        loadSchema();
    }

    private void loadSchema() {
        try (InputStream schemaStream = getClass().getClassLoader().getResourceAsStream("transaction-schema.json")) {
            if (schemaStream == null) {
                throw new RuntimeException("JSON-схема не найдена");
            }
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
            schema = factory.getSchema(schemaStream);
            log.info("JSON-схема загружена.");
        } catch (Exception e) {
            log.error("Ошибка загрузки JSON-схемы: {}", e.getMessage(), e);
            throw new RuntimeException("Ошибка при загрузке схемы", e);
        }
    }

    @Override
    public byte[] serialize(String topic, Transaction data) {
        try {
            String json = objectMapper.writeValueAsString(data);
            Set<ValidationMessage> validationResult = schema.validate(objectMapper.readTree(json));

            if (!validationResult.isEmpty()) {
                log.error("Ошибка валидации JSON: {}", validationResult);
                throw new SerializationException("Ошибка валидации JSON: " + validationResult);
            }

            return json.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error(" Ошибка сериализации транзакции: {}", e.getMessage(), e);
            throw new SerializationException("Ошибка сериализации", e);
        }
    }

    @Override
    public void close() {
    }
}
