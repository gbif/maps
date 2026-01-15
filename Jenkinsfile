pipeline {
  agent any
  tools {
    maven 'Maven 3.8.5'
    jdk 'OpenJDK17'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    skipStagesAfterUnstable()
    timestamps()
    disableConcurrentBuilds()
  }
  parameters {
    booleanParam(name: 'RELEASE', defaultValue: false, description: 'Make a Maven release')
  }
  stages {
    stage('Validate') {
      when {
        allOf {
          expression { params.RELEASE }
          not {
             branch 'master'
          }
        }
      }
      steps {
        script {
          error('Releases are only allowed from the master branch.')
        }
      }
    }
    stage('Set project version') {
      steps {
        script {
          env.VERSION = """${sh(returnStdout: true, script: './build/get-version.sh ${RELEASE}')}"""
        }
      }
    }
    stage('Maven build') {
      when {
        allOf {
          not { expression { params.RELEASE } };
        }
      }
      steps {
        configFileProvider([configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')]) {
          sh 'mvn -s $MAVEN_SETTINGS clean verify deploy -B -U'
        }
      }
    }
    stage('Release version to nexus') {
      when {
        allOf {
          expression { params.RELEASE }
          branch 'master'
        }
      }
      steps {
        configFileProvider([configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')]) {
          git 'https://github.com/gbif/maps.git'
          sh 'mvn -s $MAVEN_SETTINGS release:prepare release:perform -Denforcer.skip=true -DskipTests'
        }
      }
    }
    stage('Build and publish Docker image') {
      steps {
        sh 'build/spark-generate-maps-docker-build.sh ${RELEASE} ${VERSION}'
      }
    }
  }
  post {
    success {
      echo 'Pipeline executed successfully!'
    }
    failure {
      echo 'Pipeline execution failed!'
    }
  }
}
