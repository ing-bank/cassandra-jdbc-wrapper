/*
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.ing.data.cassandra.jdbc.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CustomObject {

    @JsonProperty("asciivalue")
    private String asciiValue;
    @JsonProperty("bigintvalue")
    private Long bigintValue;
    @JsonProperty("blobvalue")
    private ByteBuffer blobValue;
    @JsonProperty("boolvalue")
    private Boolean boolValue;
    @JsonProperty("datevalue")
    private LocalDate dateValue;
    @JsonProperty("decimalvalue")
    private BigDecimal decimalValue;
    @JsonProperty("doublevalue")
    private Double doubleValue;
    @JsonProperty("floatvalue")
    private Float floatValue;
    @JsonProperty("inetvalue")
    private InetAddress inetValue;
    @JsonProperty("intvalue")
    private Integer intValue;
    @JsonProperty("listvalue")
    private List<Integer> listValue;
    @JsonProperty("mapvalue")
    private Map<Integer, String> mapValue;
    @JsonProperty("smallintvalue")
    private Short smallintValue;
    @JsonProperty("setvalue")
    private Set<Integer> setValue;
    @JsonProperty("textvalue")
    private String textValue;
    @JsonProperty("timevalue")
    private LocalTime timeValue;
    @JsonProperty("tsvalue")
    private OffsetDateTime tsValue;
    @JsonProperty("timeuuidvalue")
    private UUID timeuuidValue;
    @JsonProperty("tinyintvalue")
    private Byte tinyintValue;
    @JsonProperty("tuplevalue")
    private List<String> tupleValue;
    @JsonProperty("uuidvalue")
    private UUID uuidValue;
    @JsonProperty("varcharvalue")
    private String varcharValue;
    @JsonProperty("varintvalue")
    private BigInteger varintValue;

}
