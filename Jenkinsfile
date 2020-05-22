pipeline {
    agent {
        node {
            label 'java'
        }
    }
    stages {
        stage ('Initialize') {
            steps {
                sh '''
                    echo "PATH = ${PATH}"
                    echo "M2_HOME = ${M2_HOME}"
                '''
            }
        }

        stage ('Build') {
            steps {
                configFileProvider(
                        [configFile(fileId: 'maven-settings', variable: 'MAVEN_SETTINGS')]) {
                    ansiColor('xterm') {
                        sh 'JAVA_HOME=/opt/openjdk1.11.0 mvn clean test -s ${MAVEN_SETTINGS} -P ci-internal -Dserver.username=${USERNAME_VAR} -Dserver.password=${PASSWORD_VAR} -q'
                    }
                }
            }
        }

        stage ('Deploy') {
            when {
                anyOf {
                    branch pattern: 'master', comparator: 'EQUALS'
                    buildingTag()
                }
            }
            steps {
                configFileProvider(
                        [configFile(fileId: 'maven-settings', variable: 'MAVEN_SETTINGS')]) {
                    ansiColor('xterm') {
                        sh 'JAVA_HOME=/opt/openjdk1.11.0 mvn deploy -s ${MAVEN_SETTINGS}  -P ci-internal -q'
                    }
                }
            }
        }
    }
// post {
//     success {
//         junit 'target/surefire-reports/**/*.xml'
//     }
// }
}
