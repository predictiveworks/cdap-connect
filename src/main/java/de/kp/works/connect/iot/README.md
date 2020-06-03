# (Industrial) Internet-of-Things Connectors

## Eclipse Ditto

**Eclipse Ditto** is an IoT technology that implements "digital twin" pattern. Such a "twin" is is a virtual, cloud based, representation of its physical or real world counterpart (charging stations, connected cars, sensors, smart heating, smart grids and more).

Eclipse Ditto is made to mirror potentially millions and billions of digital twins and thereby abstracts & unifies 
access and turns physical things into just another (web) service.

Eclipse Ditto is neither an IoT platform nor software that runs on an IoT gateway. It is designed as an intermediate layer to unify access to already connected devices, e.g. via **Eclipse Hono**). 

CDAP Connect uses Ditto's web socket API to subscribe to twin change events and live messages, and exposes them as Apache Spark real-time stream. The CDAP compliant [StreamingSource] plugin transforms events into structured records & publishes them to the subsequent pipeline stages.  

<img src="https://github.com/predictiveworks/cdap-connect/blob/master/images/ditto-connect.png" width="800" alt="Eclipse Ditto Connect">

## Eclipse Paho

## HiveMQ

## The Things Network

## ThingsBoard
