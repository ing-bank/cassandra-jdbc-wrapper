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
package com.ing.data.cassandra.jdbc.testing;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CustomObjectStringOnly {

    @JsonProperty("asciivalue")
    private String asciiValue;
    @JsonProperty("bigintvalue")
    private String bigintValue;
    @JsonProperty("blobvalue")
    private String blobValue;
    @JsonProperty("boolvalue")
    private String boolValue;
    @JsonProperty("datevalue")
    private String dateValue;
    @JsonProperty("decimalvalue")
    private String decimalValue;
    @JsonProperty("doublevalue")
    private String doubleValue;
    @JsonProperty("floatvalue")
    private String floatValue;
    @JsonProperty("inetvalue")
    private String inetValue;
    @JsonProperty("intvalue")
    private String intValue;
    @JsonProperty("listvalue")
    private List<String> listValue;
    @JsonProperty("mapvalue")
    private Map<String, String> mapValue;
    @JsonProperty("smallintvalue")
    private String smallintValue;
    @JsonProperty("setvalue")
    private List<String> setValue;
    @JsonProperty("textvalue")
    private String textValue;
    @JsonProperty("timevalue")
    private String timeValue;
    @JsonProperty("tsvalue")
    private String tsValue;
    @JsonProperty("timeuuidvalue")
    private String timeuuidValue;
    @JsonProperty("tinyintvalue")
    private String tinyintValue;
    @JsonProperty("tuplevalue")
    private List<String> tupleValue;
    @JsonProperty("uuidvalue")
    private String uuidValue;
    @JsonProperty("varcharvalue")
    private String varcharValue;
    @JsonProperty("varintvalue")
    private String varintValue;

}
