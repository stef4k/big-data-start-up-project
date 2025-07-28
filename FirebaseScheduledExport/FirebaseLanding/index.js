const { initializeApp } = require('firebase-admin/app');
const admin = require('firebase-admin');
const { Firestore } = require('@google-cloud/firestore');

// Initialize Firebase Admin
initializeApp();

exports.scheduledFirebaseExport = async (req, res) => {
  try {
    const bucketName = 'group-1-landing-lets-talk'; // Your bucket name
    const databaseId = '(default)'; // Default database
    
    // Create Firestore Admin Client
    const firestoreAdmin = new Firestore.v1.FirestoreAdminClient();
    
    // Format date as YYYY-MM-DD
    const today = new Date();
    const dateString = today.toISOString().split('T')[0];
    const outputUriPrefix = `gs://${bucketName}/users/${dateString}/`;
    
    console.log(`Starting export to ${outputUriPrefix}`);
    
    const projectId = 'calm-depot-454710-p0';
    
    const [operation] = await firestoreAdmin.exportDocuments({
      name: `projects/${projectId}/databases/${databaseId}`,
      outputUriPrefix: outputUriPrefix,
      collectionIds: ['private_users', 'public_users', 'tech_dummy'] // Specific collections
    });
    
    console.log('Export operation started:', operation.name);
    res.status(200).json({
      status: 'success',
      operation: operation.name,
      outputUri: outputUriPrefix,
      collections: ['private_users', 'public_users', 'tech_dummy']
    });
  } catch (error) {
    console.error('Export failed:', error);
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
};