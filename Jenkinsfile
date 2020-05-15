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
                withCredentials([usernamePassword(credentialsId: 'nexus-sysarch-deploy', passwordVariable: 'PASSWORD_VAR', usernameVariable: 'USERNAME_VAR')])
                {
                    sh 'JAVA_HOME=/opt/openjdk1.11.0 mvn clean test -s settings.xml -P ci-internal -Dserver.username=${USERNAME_VAR} -Dserver.password=${PASSWORD_VAR} -q'
                }
            }
        }

        stage ('Deploy') {
            when {
                anyOf{
                    branch pattern: "master", comparator: "EQUALS"
                    buildingTag()
                }
            }
            steps {
                withCredentials([usernamePassword(credentialsId: 'nexus-sysarch-deploy', passwordVariable: 'PASSWORD_VAR', usernameVariable: 'USERNAME_VAR')])
                {
                    sh 'JAVA_HOME=/opt/openjdk1.11.0 mvn deploy -s settings.xml -P ci-internal -Dserver.username=${USERNAME_VAR} -Dserver.password=${PASSWORD_VAR} -q'
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
