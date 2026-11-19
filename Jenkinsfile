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
          // Branch and PR builds get a branch-qualified version (workspace only, never committed), so their
          // artifacts and Docker images don't overwrite the dev snapshots, e.g. 1.1.16-FIX-SECURITY-REVIEW-SNAPSHOT
          if (env.BRANCH_NAME && env.BRANCH_NAME != 'dev' && env.BRANCH_NAME != 'master' && !params.RELEASE) {
            def suffix = env.BRANCH_NAME.replaceAll('[^a-zA-Z0-9.-]', '-').toUpperCase()
            sh """
              BASE_VERSION=\$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout | sed 's/-SNAPSHOT//')
              mvn versions:set -DnewVersion=\${BASE_VERSION}-${suffix}-SNAPSHOT -DgenerateBackupPoms=false -DprocessAllModules=true
            """
          }
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


    stage('Trigger WS deploy dev') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          branch 'dev';
        }
      }
      steps {
        build job: "maps-vectortile-server-adhoc-dev-deploy", wait: true, propagate: false
        build job: "maps-vectortile-server-density-dev-deploy", wait: true, propagate: false
        build job: "maps-mapnik-server-dev-deploy", wait: true, propagate: false
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
    cleanup {
      deleteDir()
    }
  }
}
