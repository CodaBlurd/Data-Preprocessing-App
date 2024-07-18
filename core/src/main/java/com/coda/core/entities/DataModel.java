package com.coda.core.entities;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * DataModel class to hold the data set model.
 * <p> This class is responsible for
 * holding the data set model to be cleaned
 * </p>
 * @param <T> the type of the data set model

 */
@Slf4j
@Document(collection = "dataModel")
@Data
@NoArgsConstructor
public class DataModel<T> implements Serializable {

    /**
     * The versionID for object serialization.
     */
    @Serial
    private static final long serialVersionUID = 1L;
    /**
     * The id of the data model.
     */
    @Id
    private ObjectId id;

    /**
     * The attributes of the data model.
     */
    private Map<String, DataAttributes<T>> attributesMap
            = new HashMap<>();
    // example attributes map data = { "id" : { "value" : 1, "type" : "int" } ,
    // "Name" : { "value" : "John", "type" : "String"}, "Age" : { "value" : 25, "type" : "int"}

//    Extracted data: DataModel{id=668af80ece6b434f197e58ba,
//            attributesMap={is_active=DataAttributes[validationRulesList=[], value=false,
//            type='java.lang.Object', attributeName='is_active', format='null', required=false,
//            defaultValue=null, description='null', validationRules='null', parsedRules=[], metadata={},
//            lastUpdatedDate=2024-07-07T20:18:22.479550Z, typeClazzName='java.lang.Object'],
//        join_date=DataAttributes[validationRulesList=[], value=2031-08-31, type='java.lang.Object',
//                attributeName='join_date', format='null', required=false, defaultValue=null,
//                description='null', validationRules='null', parsedRules=[], metadata={}, l
//        astUpdatedDate=2024-07-07T20:18:22.479889Z, typeClazzName='java.lang.Object'],
//        name=DataAttributes[validationRulesList=[], value=David, type='java.lang.String',
//                attributeName='name', format='null', required=false, defaultValue=null, description='null',
//                validationRules='null', parsedRules=[], metadata={},
//                lastUpdatedDate=2024-07-07T20:18:22.482255Z, typeClazzName='java.lang.String'],
//        id=DataAttributes[validationRulesList=[], value=0, type='java.lang.Integer', attributeName='id',
//                format='null', required=false, defaultValue=null, description='null', validationRules='null',
//                parsedRules=[], metadata={}, lastUpdatedDate=2024-07-07T20:18:22.487821Z,
//            typeClazzName='java.lang.Integer'], salary=DataAttributes[validationRulesList=[],
//        value=0.0, type='java.math.BigDecimal', attributeName='salary', format='null',
//                required=false, defaultValue=null, description='null', validationRules='null',
//                parsedRules=[], metadata={}, lastUpdatedDate=2024-07-07T20:18:22.489799Z,
//            typeClazzName='java.math.BigDecimal'], department=DataAttributes[validationRulesList=[],
//        value=Engineering, type='java.lang.String', attributeName='department', format='null', required=false,
//                defaultValue=null, description='null', validationRules='null', parsedRules=[], metadata={},
//            lastUpdatedDate=2024-07-07T20:18:22.490120Z, typeClazzName='java.lang.String'],
//        age=DataAttributes[validationRulesList=[], value=0, type='java.lang.Integer',
//                attributeName='age', format='null', required=false, defaultValue=null,
//                description='null', validationRules='null', parsedRules=[], metadata={},
//                astUpdatedDate=2024-07-07T20:18:22.491145Z, typeClazzName='java.lang.Integer']}}
    /**
     * TimeStamp.
     */

    @CreatedDate
    private Instant createdDate;

    /**
     * TimeStamp.
     */

    @LastModifiedDate
    private Instant updatedAt;



    /**
     * Constructor for DataModel.
     * @param dataModelId the id of the data model.
     * @param attr the attributes of the data model.
     */

    public DataModel(final ObjectId dataModelId,
                     final Map<String, DataAttributes<T>> attr) {
        this.id = dataModelId;
        this.attributesMap = attr != null ? attr : new HashMap<>();
    }

    @Override
    public final String toString() {
        StringJoiner attributesJoiner
                = new StringJoiner(", ",
                "{", "}");

        for (Map.Entry<String, DataAttributes<T>>
                entry: attributesMap.entrySet()) {

            attributesJoiner.add(entry.getKey()
                    + "=" + entry.getValue());

        }

        return "DataModel{"
                + "id="
                + id
                + ", attributesMap="
                + attributesJoiner
                + '}';
    }
}


