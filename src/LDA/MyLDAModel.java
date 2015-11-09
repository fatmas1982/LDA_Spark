package LDA;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.spark.mllib.linalg.Matrix;

public class MyLDAModel implements Serializable {

	public double myalpha;
	public double[][] topicTermsMatrix;
	public int numTopics;
	public int numTerms;
	
	MyLDAModel()
	{
		myalpha = 0;
		topicTermsMatrix = null;
		numTopics = 0;
		numTerms = 0;
	}
	
	public MyLDAModel(double alp, Matrix tTMatrix, int k, int vocabsize)
	{	
		myalpha = alp;
		numTopics = k;
		numTerms = vocabsize;
		double tempsum[] = new double[k];
		topicTermsMatrix = new double [vocabsize][k];
		for (int i=0; i<k; i++)
		{
			tempsum[i] = 0;
		}
		for (int word = 0; word < vocabsize; word++) {
			for (int topic = 0; topic < k; topic++) {
				topicTermsMatrix[word][topic] = tTMatrix.apply(word, topic);
				tempsum[topic] = tempsum[topic] + topicTermsMatrix[word][topic];
			}
		}
		for (int word = 0; word < vocabsize; word++) {
			for (int topic = 0; topic < k; topic++) {
				topicTermsMatrix[word][topic] = topicTermsMatrix[word][topic] / tempsum[topic];				
			}
		}
	}
	
	public void saveTopicTermMatrix(String path) throws IOException
	{
		FileOutputStream out = new FileOutputStream(new File(path));
		for (int topic = 0; topic < numTopics; topic++)
		{
			for(int word = 0; word < numTerms; word++)
			{
				String temp = String.valueOf(topicTermsMatrix[word][topic]);
				out.write((temp+" ").getBytes());
			}
			out.write("\r\n".getBytes());
		}
		out.close();
	}
	
	public int k()
	{
		return numTopics;
	}
	
	public int vocabSize()
	{
		return numTerms;
	}
	
	public double[][] topicsMatrix()
	{
		return topicTermsMatrix;
	}
	
	public double digamma(double x)
	{
		double p = 0.0;
	    x = x + 6;
		p = 1 / ( x * x );
		p = ( ( ( 0.004166666666667 * p - 0.003968253986254 ) * p + 0.008333333333333 ) * p - 0.083333333333333 ) * p ;
		p = p + Math.log(x) - 0.5 / x - 1 / ( x - 1 ) - 1 / ( x - 2 ) - 1 / ( x - 3 ) - 1 / ( x - 4 ) - 1 / ( x - 5 ) - 1 / ( x - 6 );
		
		return p;
	}
	
	public double log_sum( double log_a, double log_b )
	{
	    double v = 0.0;    
	    if (log_a < log_b)
	        v = log_b + Math.log( 1 + Math.exp( log_a - log_b ) );
	    else
	        v = log_a + Math.log( 1 + Math.exp( log_b - log_a ) );
	    return v;
	}
	
	public double log_gamma(double x)
	{
	    double z=1/(x*x);
	    x=x+6;
	    z=(((-0.000595238095238*z+0.000793650793651)*z-0.002777777777778)*z+0.083333333333333)/x;
	    z=(x-0.5)*Math.log(x)-x+0.918938533204673+z-Math.log(x-1)-Math.log(x-2)-Math.log(x-3)-Math.log(x-4)-Math.log(x-5)-Math.log(x-6);
	    return z;
	}
	
	public double compute_likelihood(org.apache.spark.mllib.linalg.Vector usr, double[] var_gamma, double[][] phi)
	{
		double likelihood = 0.0;
		double digsum = 0.0;
		double var_gamma_sum = 0.0;
		double[] dig = new double[numTopics];
		int k, n;

	    for (k = 0; k < numTopics; k++)
	    {
		    dig[k] = digamma(var_gamma[k]);
		    var_gamma_sum += var_gamma[k];
	    }
	    digsum = digamma(var_gamma_sum);

	    likelihood = log_gamma(myalpha * numTopics - numTopics * log_gamma(myalpha) - (log_gamma(var_gamma_sum)));

	    for (k = 0; k < numTopics; k++)
	    {
		    likelihood += (myalpha - 1)*(dig[k] - digsum) + log_gamma(var_gamma[k]) - (var_gamma[k] - 1)*(dig[k] - digsum);
		    for (n = 0; n < numTerms; n++)
		    {
	            if (phi[n][k] > 0)
	            {
	                likelihood += (double)usr.apply(n) * ( phi[n][k] * ( (dig[k] - digsum) - Math.log(phi[n][k]) + Math.log(topicTermsMatrix[n][k])));
	            }
	        }
	    }
	    return(likelihood);
	}
	
	public double[] predict(org.apache.spark.mllib.linalg.Vector usr)
	{
		// Calculate the User - cate distribution
		double[] var_gamma = new double[numTopics];
		double converged = 1.0;
		double phisum = 0.0;
		double likelihood = 0.0;
		double likelihood_old = 0.0;
		double[][] phi = new double[numTerms][numTopics];
		double[] oldphi = new double[numTopics];
		double[] digamma_gam = new double[numTopics];
		
		for(int k = 0; k < numTopics; k++)
		{
			var_gamma[k] = myalpha + numTerms / numTopics;
			digamma_gam[k] = digamma( var_gamma[k] );
			for(int n = 0; n < numTerms; n++)
				phi[n][k] = 1.0/numTopics;
		}	
		int var_iter = 0;
		while(converged > 1e-6)
		{
			var_iter += 1;
			for(int n = 0; n < numTerms; n++)
			{
				phisum = 0;
				for(int k = 0; k < numTopics; k++)
				{
					oldphi[k] = phi[n][k];
					phi[n][k] = digamma_gam[k] + Math.log(topicTermsMatrix[n][k]); // log_prob?
					if(k > 0)
						phisum = log_sum(phisum, phi[n][k]);
					else
						phisum = phi[n][k];
				}
				for(int k = 0; k < numTopics; k++)
				{
					phi[n][k] = Math.exp( phi[n][k] - phisum );
			        var_gamma[k] = var_gamma[k] + (double)usr.apply(n) * ( phi[n][k] - oldphi[k] );
			        digamma_gam[k] = digamma( var_gamma[k] );
				}
			}
			likelihood = compute_likelihood(usr, var_gamma, phi);		        
			if (1 != var_iter)
			{
				converged = ( likelihood_old - likelihood ) / likelihood_old;
				likelihood_old = likelihood;
			}
		}	
		return var_gamma;
	}
	
	public void save(String path) throws IOException
	{
		ObjectOutputStream objout = new ObjectOutputStream(new FileOutputStream(path));
		objout.writeObject(this);
		objout.close();
	}
	
}
