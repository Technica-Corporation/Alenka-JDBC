/*
*Alenka JDBC - A JDBC Driver for Alenka
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/

package com.technicacorp.alenkajdbc;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

public class AlenkaConfig {


    /**
     * Default constructor.
     */
    public AlenkaConfig() {
        this(new Properties());
    }

    /**
     * Creates an Alenka configuration object using values from the given
     * property object.
     *
     * @param prop The properties to apply to the configuration.
     */
    public AlenkaConfig(Properties prop) {

    }

    /**
     * @return Array of DriverPropertyInfo objects.
     */
    static DriverPropertyInfo[] getDriverPropertyInfo() {
        return null;
    }
}



