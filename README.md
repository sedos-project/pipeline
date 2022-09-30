# SEDOS Pipeline

The SEDOS Pipeline gathers data from AP1-AP4, maps data to OEDatamodel and builds a reference scenario for an energysystem simulation.
Steps are:
- Check for updates on registered artefacts on Databus (from collection)
- If updates are available, download new artefacts (ParameterModel)
- Transform downloaded artefacts into OEDatamodel
- Upload OEDatamodel data to OEP
- Register new data on Databus

After those steps, a Databus collection should be available, holding OEDatamodel data for reference scenario.
