from tornado.web import HTTPError
from json import dumps

from qiita_pet.handlers.base_handlers import BaseHandler
from qiita_db.study import Study
from qiita_db.exceptions import QiitaDBUnknownIDError


class StudyAbstractAPIHandler(BaseHandler):
    """Handler to retrieve study abstract - NO AUTHENTICATION REQUIRED"""
    
    def get(self, study_id):
        """
        Retrieve the abstract for a study
        
        Parameters
        ----------
        study_id : str
            The study ID
        
        Returns
        -------
        JSON object with abstract
        """
        try:
            study_id = int(study_id)
            study = Study(study_id)
            
            # Build response with available information
            response = {
                'study_id': study_id,
                'title': study.title,
                'abstract': study.info.get('study_abstract', 'No abstract available'),
                'status': study.status
            }
            
            self.set_header('Content-Type', 'application/json')
            self.write(dumps(response))
            
        except QiitaDBUnknownIDError:
            self.set_status(404)
            self.write(dumps({'error': f'Study {study_id} not found'}))
        except ValueError:
            self.set_status(400)
            self.write(dumps({'error': 'Invalid study ID format'}))
        except AttributeError as e:
            # This catches the 'level' error you're seeing
            self.set_status(500)
            self.write(dumps({'error': f'AttributeError: {str(e)}. Study object may not be fully initialized.'}))
        except Exception as e:
            self.set_status(500)
            self.write(dumps({'error': f'Unexpected error: {str(e)}'}))


class StudyDetailAPIHandler(BaseHandler):
    """Handler to retrieve detailed study information - NO AUTHENTICATION"""
    
    def get(self, study_id):
        """
        Retrieve study details including author and abstract
        
        Parameters
        ----------
        study_id : str
            The study ID to retrieve
        
        Returns
        -------
        JSON object with study details
        """
        try:
            study_id = int(study_id)
            study = Study(study_id)
            
            # Safely get nested dictionary values
            info = study.info if hasattr(study, 'info') else {}
            
            # Build the response with study details
            response = {
                'study_id': study_id,
                'title': study.title if hasattr(study, 'title') else 'No title',
                'abstract': info.get('study_abstract', 'No abstract available'),
                'description': info.get('study_description', 'No description available'),
                'status': study.status if hasattr(study, 'status') else 'unknown'
            }
            
            # Try to get PI info safely
            if 'principal_investigator' in info:
                pi = info['principal_investigator']
                response['principal_investigator'] = {
                    'name': pi.get('name', '') if isinstance(pi, dict) else str(pi),
                    'email': pi.get('email', '') if isinstance(pi, dict) else '',
                    'affiliation': pi.get('affiliation', '') if isinstance(pi, dict) else ''
                }
            
            # Try to get lab person info safely
            if 'lab_person' in info:
                lab = info['lab_person']
                response['lab_person'] = {
                    'name': lab.get('name', '') if isinstance(lab, dict) else str(lab),
                    'email': lab.get('email', '') if isinstance(lab, dict) else '',
                    'affiliation': lab.get('affiliation', '') if isinstance(lab, dict) else ''
                }
            
            # Try to get publications
            response['publication_doi'] = info.get('publication_doi', [])
            response['publication_pid'] = info.get('publication_pid', [])
            response['study_alias'] = info.get('study_alias', '')
            
            self.set_header('Content-Type', 'application/json')
            self.write(dumps(response))
            
        except QiitaDBUnknownIDError:
            self.set_status(404)
            self.write(dumps({'error': f'Study {study_id} not found'}))
        except ValueError:
            self.set_status(400)
            self.write(dumps({'error': 'Invalid study ID format'}))
        except Exception as e:
            self.set_status(500)
            self.write(dumps({'error': f'Error retrieving study: {str(e)}'}))


class StudyAuthorsAPIHandler(BaseHandler):
    """Handler to retrieve study authors specifically - NO AUTHENTICATION"""
    
    def get(self, study_id):
        """
        Retrieve only author information for a study
        
        Parameters
        ----------
        study_id : str
            The study ID to retrieve authors for
        
        Returns
        -------
        JSON object with author details
        """
        try:
            study_id = int(study_id)
            study = Study(study_id)
            
            info = study.info if hasattr(study, 'info') else {}
            
            response = {
                'study_id': study_id,
                'study_title': study.title if hasattr(study, 'title') else 'No title',
                'principal_investigator': info.get('principal_investigator', {}),
                'lab_person': info.get('lab_person', {})
            }
            
            self.set_header('Content-Type', 'application/json')
            self.write(dumps(response))
            
        except QiitaDBUnknownIDError:
            self.set_status(404)
            self.write(dumps({'error': f'Study {study_id} not found'}))
        except Exception as e:
            self.set_status(500)
            self.write(dumps({'error': f'Error retrieving authors: {str(e)}'}))


class StudyListAPIHandler(BaseHandler):
    """Handler to list all available studies - NO AUTHENTICATION"""
    
    def get(self):
        """
        List all studies in the system
        
        Returns
        -------
        JSON object with list of studies
        """
        try:
            from qiita_db.study import Study
            
            # Get all study IDs
            studies = Study.get_all_study_ids()
            
            study_list = []
            for study_id in studies:
                try:
                    study = Study(study_id)
                    study_list.append({
                        'study_id': study_id,
                        'title': study.title if hasattr(study, 'title') else 'No title',
                        'status': study.status if hasattr(study, 'status') else 'unknown'
                    })
                except:
                    # Skip studies that can't be loaded
                    continue
            
            response = {
                'total_studies': len(study_list),
                'studies': study_list
            }
            
            self.set_header('Content-Type', 'application/json')
            self.write(dumps(response))
            
        except Exception as e:
            self.set_status(500)
            self.write(dumps({'error': f'Error listing studies: {str(e)}'}))